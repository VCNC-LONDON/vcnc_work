import json
import datetime
import pandas as pd
import requests
import forecast.src.lcc as lcc

from google.cloud import bigquery
from forecast.src.weatherforecast_model import WeatherForecastModel


class WeatherForecast(WeatherForecastModel):
    BIGQUERY_PROJECT = "kr-co-vcnc-tada"
    BIGQUERY_DATASET = "tada_temp_london"
    BIGQUERY_TABLE = "weather_test"
    BIGQUERY_TIME_PARTITION_FIELD = "date_kr"
    BIGQUERY_FIELDS = [
        "date_kr",
        "h3",
        "lat",
        "lon",
        "updated_at_kr",
        "forecast_at_kr",
        "is_thunder",
        "precipitation",
        "rain",
        "sky_condition",
        "temperatures",
        "humidity_rate",
        "wind_spped_mps"
    ]
    
    def __init__(self):
        pass

    @staticmethod
    def tranform_rn1(x: str):
        result = ""
        if x == "강수없음":
            result = "01_강수없음"
        elif x == "1.0mm 미만":
            result = "02_1mm 미만"
        elif x == "30.0~50.0mm":
            result = "04_30mm 이상 50mm 미만"
        elif x == "50.0mm 이상":
            result = "05_50mm 이상"
        else:
            result = "03_1mm 이상 30mm 미만"
        return result
    
    def get_bigquery_info(self) -> dict:
        bigquery_info = {}
        bigquery_info["project"] = self.BIGQUERY_PROJECT
        bigquery_info["dataset"] = self.BIGQUERY_DATASET
        bigquery_info["table"] = self.BIGQUERY_TABLE
        bigquery_info["location"] = "US"
        bigquery_info["time_partition_field"] = self.BIGQUERY_TIME_PARTITION_FIELD
        bigquery_info["schema"] = [
            bigquery.SchemaField("date_kr", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("h3", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("lat", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("lon", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("updated_at_kr", "DATETIME", mode="NULLABLE"),
            bigquery.SchemaField("forecast_at_kr", "DATETIME", mode="NULLABLE"),
            bigquery.SchemaField("is_thunder", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("precipitation", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("rain", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("sky_condition", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("temperatures", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("humidity_rate", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("wind_spped_mps", "FLOAT", mode="NULLABLE")
        ]
        return bigquery_info

    def get_url(self, request_type: str) -> str:
        if request_type == "shorterm":
            endpoint_type = "/getVilageFcst"
        elif request_type == "hyper_shorterm":
            endpoint_type = "/getUltraSrtFcst"
        elif request_type == "hyper_shorterm_now":
            endpoint_type = "/getUltraSrtNcst"
        else:
            raise Exception("Request invalid search type")

        url = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0" + endpoint_type
        return url

    def request_api(self, request_type: str, search_target: dict) -> object:
        url = self.get_url(request_type)

        decode_api_key = "w2CX5WYoiCwUAVzySJCChBwlTu3OJwu2VIMtvRjLczfWMcRZucRqg6N96ygYzYNnfqfcLsBk+xIPhcvYL087tw=="
        base_date = 20220928
        base_time = 1400
        x, y = lcc.to_grid(search_target["lat"], search_target["lng"])

        params = {
            "serviceKey": decode_api_key,
            "pageNo": "1",
            "numOfRows": "9999",
            "dataType": "JSON",
            "base_date": str(base_date),
            "base_time": str(base_time),
            "nx": str(x),
            "ny": str(y),
        }

        response = requests.get(url, params)

        if response.status_code != 200:
            raise Exception(
                f"status code [{response.status_code}], message [{response.text}]"
            )

        data = response.json()
        data["response"]["body"]["h3"] = search_target["h3"]
        data["response"]["body"]["lat"] = search_target["lat"]
        data["response"]["body"]["lon"] = search_target["lng"]

        return data

    def parse_response(self, data: dict) -> pd.DataFrame:
        items = data["response"]["body"]

        result = []
        weather_dict = {}

        weather_dict["h3"] = items["h3"]
        weather_dict["lat"] = items["lat"]
        weather_dict["lon"] = items["lon"]

        for idx, i_value in enumerate(items["items"]["item"]):
            weather_dict = {}
            weather_dict["h3"] = items["h3"]
            weather_dict["lat"] = items["lat"]
            weather_dict["lon"] = items["lon"]

            weather_dict["date_kr"] = datetime.datetime.strptime(
                    i_value["fcstDate"], "%Y-%m-%d"
            )
            
            weather_dict["updated_at_kr"] = int(
                datetime.datetime.strptime(
                    i_value["baseDate"] + i_value["baseTime"] + "00", "%Y%m%d%H%M%S"
                ).timestamp()
            )
            weather_dict["forecast_at_kr"] = int(
                datetime.datetime.strptime(
                    i_value["fcstDate"] + i_value["fcstTime"] + "00", "%Y%m%d%H%M%S"
                ).timestamp()
            )

            weather_dict["category"] = i_value["category"]
            weather_dict["fcstValue"] = i_value["fcstValue"]

            result.append(weather_dict)

        df = pd.DataFrame(result)
        category = df["category"].unique()

        target_df = df[
            ["h3", "lat", "lon", "base_datetime", "forecasting_datetime"]
        ].drop_duplicates()

        for c_name in category:
            if c_name in ["UUU", "VVV", "VEC"]:
                pass
            else:
                df_tmp = df[df["category"] == c_name]
                df_tmp.rename(columns={"fcstValue": f"{c_name}"}, inplace=True)
                df_tmp = df_tmp.drop(columns="category")
                target_df = pd.merge(
                    target_df,
                    df_tmp,
                    on=["h3", "lat", "lon", "base_datetime", "forecasting_datetime"],
                    how="left",
                )

        target_df["LGT"] = target_df["LGT"].apply(
            lambda x: True if int(x) > 0.0 else False
        )
        target_df["PTY"] = target_df["PTY"].astype(
            "int"
        )  # 없음(0), 비(1), 비/눈(2), 눈(3), 빗방울(5), 빗방울눈날림(6), 눈날림(7)

        target_df["RN1"] = (
            target_df["RN1"].apply(WeatherForecast.tranform_rn1).astype("str")
        )
        target_df["SKY"] = target_df["SKY"].astype("int")  # 맑음(1), 구름많음(3), 흐림(4)
        target_df["T1H"] = target_df["T1H"].astype("int")
        target_df["REH"] = target_df["REH"].astype("int") / 100
        target_df["WSD"] = target_df["WSD"].astype("float")
        target_df.rename(
            columns={
                "LGT": "is_thunder",
                "PTY": "precipitation",
                "RN1": "rain",
                "SKY": "sky_condition",
                "T1H": "temperatures",
                "REH": "humidity_rate",
                "WSD": "wind_spped_mps",
            },
            inplace=True,
        )
        return target_df
