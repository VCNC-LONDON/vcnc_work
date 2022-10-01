import datetime
import requests
import json
import pandas as pd
import logging
import os
import time
import sys

import concurrent.futures as futures
from itertools import repeat

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from dependencies.weatherforecast.weather_model import WeatherModel
from dependencies.weatherforecast.lcc import LCC

LOGGER_NAME = "WeatherForecastBigqueryLoader"
custom_logger = logging.getLogger(LOGGER_NAME)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)
custom_logger.addHandler(stream_handler)
custom_logger.setLevel(logging.INFO)

lcc =LCC()

class BigqueryLoader:
    def __init__(self, load_datetime : datetime.datetime, weatherforecast_api_token : str, google_credentials : str):
        self.load_datetime = load_datetime
        self.weatherforecast_api_token = weatherforecast_api_token
        self.google_credentials = google_credentials

    def do_etl(self, request_type : str, target_model : WeatherModel) -> None :
        try:
            request_url = target_model.get_url(request_type)
            process_data = self.processing(request_url, target_model)
            self.load_to_bq(process_data, target_model)

        except Exception as err:
            logging.getLogger(LOGGER_NAME).error(f"do_etl() failed: {err}")
            raise

    def request_api(self, url: str, req_target : pd.DataFrame, idx : int) -> object:
        response = None
        KST = datetime.timezone(datetime.timedelta(hours=9))
        request_datetime = datetime.datetime.fromtimestamp(
            self.load_datetime.timestamp(), tz = KST
        )

        h3 = req_target["h3_l7"][idx]
        lat = req_target["lat"][idx]
        lon = req_target["lon"][idx]
        base_date = request_datetime.strftime("%Y%m%d") 
        base_time = request_datetime.strftime("%H%M")
        x, y = lcc.to_grid(lat, lon)

        params = {
            "serviceKey": self.weatherforecast_api_token,
            "pageNo": "1",
            "numOfRows": "9999",
            "dataType": "JSON",
            "base_date": str(base_date),
            "base_time": str(base_time),
            "nx": str(x),
            "ny": str(y),
        }
        
        # 실패하는 경우는 드물지만 상황에 따라 잦아질 때가 있다. 이럴땐 바로 에러 내지 말고 0.7초씩 쉬면서 한 번은 더 시도해본다.
        # 추후에는 아예 순회돌면서 안 된 것들만 모아서 계속 다시 돌리는 방식으로 가야할듯..row에 Null/NaN이 있는지 체크하면서 해당 경우면 다시 API 때리는 방식?
        try :
            response = requests.get(url, params)
        
            if response.status_code != 200:
                time.sleep(0.7)
                response = requests.get(url,params)
                if response.status_code != 200:
                    raise Exception(
                        f"status code [{response.status_code}], message [{response.text}]"
                    )

            data = response.json()
            data["response"]["body"]["h3_l7"] = h3
            data["response"]["body"]["lat"] = lat
            data["response"]["body"]["lon"] = lon
            data["response"]["body"]["updated_at_kr"] = request_datetime
        
        except Exception as err:
            logging.getLogger(LOGGER_NAME).error(
                f"request_api() failed: request {h3}({lat}, {lon}) has been failed...error {err}"
            )
            raise

        return data
    
    def parse_response(self, json_data : str,target_model : WeatherModel) -> object :
        if json_data is None:
            pass
        else :
            return target_model.parse_response(json_data)

    def processing(self, request_url : str, target_model: WeatherModel) -> pd.DataFrame :
        targets = self.get_search_targets(target_model)
        logging.getLogger(LOGGER_NAME).info(f"processing() : Initiate processing...search {len(targets)} area")

        io_result = []
        with futures.ThreadPoolExecutor() as thread_executor:
            for io_res in thread_executor.map(self.request_api , repeat(request_url), repeat(targets), [idx for idx in targets.index]) :
                io_result.append(io_res)
        logging.getLogger(LOGGER_NAME).info(f"processing() : API request all done...result : {len(io_result)}")

        merge_result = pd.DataFrame()
        with futures.ProcessPoolExecutor() as process_executor:
            for pp_res in process_executor.map(self.parse_response, io_result, repeat(target_model)) :
                merge_result = merge_result.append(pp_res)
        logging.getLogger(LOGGER_NAME).info(f"processing() : Merge all data. ready to load bigquery")
        
        return merge_result
    
    def load_to_bq(self, result_df : pd.DataFrame, target_model: WeatherModel):
        try:
            bigquery_info = target_model.get_bigquery_info()
            logging.getLogger(LOGGER_NAME).info(
                f"load_to_bq(): info [{bigquery_info}]"
            )

            client = bigquery.Client(
                project=bigquery_info["project"], credentials=self.google_credentials
            )

            job_config = bigquery.LoadJobConfig(
                schema=bigquery_info["schema"],
                time_partitioning = bigquery.table.TimePartitioning(
                    field=bigquery_info["time_partition_field"]
                ),
                write_disposition="WRITE_TRUNCATE",
            )

            dataset_ref = client.dataset(bigquery_info["dataset"])
            table_ref = dataset_ref.table(bigquery_info["table"])
            job = client.load_table_from_dataframe(
                dataframe = result_df,
                destination = table_ref,
                location = bigquery_info["location"],
                project=bigquery_info["project"],
                job_config = job_config
            )
            job.result()
            logging.getLogger(LOGGER_NAME).info(
                f"load_to_bq(): info [{job.output_rows}] rows loaded to {dataset_ref}.{table_ref}"
            )

        except Exception as err:
            logging.getLogger(LOGGER_NAME).error(
                f"load_to_bq() failed: {err}"
            )
            raise
    
    def is_table_existing(self, client : object, target_model: WeatherModel) -> bool:
        result = True
        bigquery_info = target_model.get_bigquery_info()

        try :
            dataset_ref = client.dataset(bigquery_info["dataset"])
            table_ref = dataset_ref.table(bigquery_info["table"])
            client.get_table(table_ref)
        
        except NotFound:
            result = False
        except Exception as err:
            logging.getLogger(LOGGER_NAME).error(f"is_table_existing() failed: {err}")
            raise
        return result
    
    def get_search_targets(self, target_model : WeatherModel) -> pd.DataFrame:
        bigquery_info = target_model.get_bigquery_info()
        client = bigquery.Client(
            project=bigquery_info["project"], credentials=self.google_credentials
        )
        is_table_existing = self.is_table_existing(client, target_model)

        if is_table_existing:
            logging.getLogger(LOGGER_NAME).info(f"get_search_targets() : target table already exists... get search_targets from table")

            bigquery_info = target_model.get_bigquery_info()
            sql = f"""
            SELECT
                DISTICNT h3_l7,
                lat,
                lon
            FROM {bigquery_info["dataset"]}.{bigquery_info["table"]}
            """
            df = client.query(sql, project=bigquery_info["project"]).to_dataframe()
            
        else :
            logging.getLogger(LOGGER_NAME).info(f"get_search_targets() : target table doesn't exist... get search_targets from configuration")

            path = os.path.join(
                os.environ["DAGS_FOLDER"],"dependencies/weatherforecast/weather_forecast_base_search_region.json"
            )
            df = pd.read_json(path)
        
        return df
