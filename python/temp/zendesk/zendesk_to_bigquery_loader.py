import base64
import http.client
import io
import json
import logging
from logging import Formatter
import os
import requests
import sys
import time

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

from dependencies.zendesk.zendesk_model import ZendeskModel

LOGGER_NAME = "ZendeskToBigqueryLoader"
custom_logger = logging.getLogger(LOGGER_NAME)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(
    Formatter("%(asctime)s %(levelname)s: %(message)s in %(filename)s:%(lineno)d]")
)
stream_handler.setLevel(logging.INFO)
custom_logger.addHandler(stream_handler)
custom_logger.setLevel(logging.INFO)


class ZendeskToBigqueryLoader:
    # 2020.11.01 00:00:00 KST -> 1604156400
    VCNC_SERVICE_INIT_TIMESTAMP = 1604156400

    def __init__(self, zendesk_api_token: str, google_credentials: str):
        # Zendesk api token string: base64[{username}/token:{api_token}]
        self.zendesk_api_token = zendesk_api_token
        self.google_credentials = google_credentials

    def do_etl(
        self,
        request_type: str,
        etl_start_timestamp: int,
        etl_end_timestamp: int,
        target_model: ZendeskModel,
    ) -> None:
        try:
            outbound_ip = requests.get("https://api.ipify.org").text
            logging.getLogger(LOGGER_NAME).info(
                f"do_etl(): outbound ip [{outbound_ip}]"
            )

            # 빅쿼리에 table이 없는 경우 (처음 Zendesk item 적재 시), 시작 시간을 조정
            is_table_existing = self.is_table_existing(target_model)
            if not is_table_existing:
                etl_start_timestamp = self.VCNC_SERVICE_INIT_TIMESTAMP
                logging.getLogger(LOGGER_NAME).info(
                    f"do_etl(): first ETL! -> etl_start_timestamp is changed [{etl_start_timestamp}]"
                )

            request_url = target_model.get_request_url(
                request_type, etl_start_timestamp
            )

            total_dict_results = []
            response = self.request_to_zendesk_api(request_url)
            next_request_url, dict_results = self.parse_response(
                request_type, response.json(), target_model
            )
            if len(dict_results) == 0:
                logging.getLogger(LOGGER_NAME).info(
                    f"do_etl(): len(dict_results) is 0..."
                )
                return
            total_dict_results.extend(dict_results)

            # 다음 page가 있는 경우, 반복 요청
            while next_request_url:
                time.sleep(10)
                response = self.request_to_zendesk_api(next_request_url)
                next_request_url, dict_results = self.parse_response(
                    request_type, response.json(), target_model
                )
                if len(dict_results) == 0:
                    break
                logging.getLogger(LOGGER_NAME).info(
                    f"do_etl(): result size [{len(dict_results)}]"
                )
                total_dict_results.extend(dict_results)

            # distinct by id
            total_dict_results = list(
                {
                    dict_result["id"]: dict_result for dict_result in total_dict_results
                }.values()
            )
            logging.getLogger(LOGGER_NAME).info(
                f"do_etl(): total [{len(total_dict_results)}] rows were downloaded"
            )

            self.load_data_to_bigquery(
                total_dict_results, etl_start_timestamp, etl_end_timestamp, target_model
            )

        except Exception as err:
            logging.getLogger(LOGGER_NAME).error(f"do_etl() failed: {err}")
            raise

    def request_to_zendesk_api(self, request_url: str) -> object:
        response = None
        try:
            # token string: base64[{username}/token:{api_token}]
            auth = f"Basic {self.zendesk_api_token}"
            headers = {"Content-Type": "application/json", "Authorization": auth}
            response = requests.get(request_url, headers=headers)
            logging.getLogger(LOGGER_NAME).info(
                f"request_to_zendesk_api(): url[{request_url}]"
            )

            if response.status_code != 200:
                raise Exception(
                    f"status code [{response.status_code}], message [{response.text}]"
                )

        except Exception as err:
            logging.getLogger(LOGGER_NAME).error(
                f"request_to_zendesk_api() failed: {err}"
            )
            raise

        return response

    def parse_response(
        self, request_type: str, json_data: str, target_model: ZendeskModel
    ) -> str:
        return target_model.parse_response(request_type, json_data)

    def load_data_to_bigquery(
        self,
        dict_results: list,
        etl_start_timestamp: int,
        etl_end_timestamp: int,
        target_model: ZendeskModel,
    ) -> None:
        temp_table = None
        try:
            is_table_existing = self.is_table_existing(target_model)
            bigquery_info = target_model.get_bigquery_info()
            temp_table = bigquery_info["table"] + f"_{str(etl_start_timestamp)}"
            if not is_table_existing:
                # 대상 테이블에 직접 입력
                self.load_data_to_bigquery_core(
                    dict_results, target_model, bigquery_info["table"]
                )
            else:
                # 임시 테이블에 load 후, 대상 테이블과 merge
                self.load_data_to_bigquery_core(dict_results, target_model, temp_table)
                self.merge_temp_table_to_target(
                    etl_start_timestamp, etl_end_timestamp, target_model, temp_table
                )
        except Exception as err:
            raise
        finally:
            self.remove_temp_table(target_model, temp_table)

    def load_data_to_bigquery_core(
        self, dict_results: list, target_model: ZendeskModel, temp_table: str
    ) -> None:
        try:
            bigquery_info = target_model.get_bigquery_info()
            logging.getLogger(LOGGER_NAME).info(
                f"load_data_to_bigquery_core(): info [{bigquery_info}]"
            )

            client = bigquery.Client(
                project=bigquery_info["project"], credentials=self.google_credentials
            )

            data_str = "\n".join(json.dumps(item) for item in dict_results)
            encoded_str = data_str.encode()
            data_file = io.BytesIO(encoded_str)

            dataset_ref = client.dataset(bigquery_info["dataset"])
            table_ref = dataset_ref.table(temp_table)
            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            job_config.time_partitioning = bigquery.table.TimePartitioning(
                field=bigquery_info["time_partition_field"]
            )
            job_config.schema = bigquery_info["schema"]
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

            job = client.load_table_from_file(
                data_file,
                table_ref,
                size=len(encoded_str),
                location=bigquery_info["location"],
                project=bigquery_info["project"],
                job_config=job_config,
            )
            job.result()
            logging.getLogger(LOGGER_NAME).info(
                f"load_data_to_bigquery_core() succeeded: [{job.output_rows}] rows loaded to {dataset_ref}.{table_ref}"
            )
        except Exception as err:
            logging.getLogger(LOGGER_NAME).error(
                f"load_data_to_bigquery_core() failed: {err}"
            )
            raise

    def merge_temp_table_to_target(
        self,
        etl_start_timestamp,
        etl_end_timestamp,
        target_model: ZendeskModel,
        temp_table: str,
    ):
        try:
            bigquery_info = target_model.get_bigquery_info()
            source_dataset = bigquery_info["dataset"]
            merge_query = target_model.make_merge_query(
                source_dataset, temp_table, etl_start_timestamp, etl_end_timestamp
            )
            logging.getLogger(LOGGER_NAME).info(f"merge query: [{merge_query}]")

            client = bigquery.Client(
                project=bigquery_info["project"], credentials=self.google_credentials
            )
            query_job = client.query(merge_query)
            results = query_job.result()
            logging.getLogger(LOGGER_NAME).info(
                f"merge_temp_table_to_target() succeeded: [{query_job.num_dml_affected_rows}] rows loaded from {source_dataset}.{temp_table}"
            )
        except Exception as err:
            logging.getLogger(LOGGER_NAME).error(
                f"merge_temp_table_to_target() failed: {err}"
            )
            raise

    def is_table_existing(self, target_model) -> bool:
        result = True
        try:
            bigquery_info = target_model.get_bigquery_info()
            client = bigquery.Client(
                project=bigquery_info["project"], credentials=self.google_credentials
            )
            dataset_ref = client.dataset(bigquery_info["dataset"])
            table_ref = dataset_ref.table(bigquery_info["table"])
            client.get_table(table_ref)
        except NotFound:
            result = False
        except Exception as err:
            logging.getLogger(LOGGER_NAME).error(f"is_table_existing() failed: {err}")
            raise
        return result

    def remove_temp_table(self, target_model: ZendeskModel, temp_table: str) -> None:
        try:
            logging.getLogger(LOGGER_NAME).info(f"remove_temp_table() [{temp_table}]")
            bigquery_info = target_model.get_bigquery_info()
            client = bigquery.Client(
                project=bigquery_info["project"], credentials=self.google_credentials
            )
            dataset_ref = client.dataset(bigquery_info["dataset"])
            table_ref = dataset_ref.table(temp_table)
            client.delete_table(table_ref, not_found_ok=True)
            logging.getLogger(LOGGER_NAME).info(
                f"remove_temp_table() succeeded: {table_ref} removed"
            )
        except Exception as err:
            logging.getLogger(LOGGER_NAME).error(f"remove_temp_table() failed: {err}")
