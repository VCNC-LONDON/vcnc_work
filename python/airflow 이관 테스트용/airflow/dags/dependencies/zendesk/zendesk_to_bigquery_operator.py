import datetime as dt
import os

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

# from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from dependencies.zendesk.zendesk_to_bigquery_loader import ZendeskToBigqueryLoader


class ZendeskToBigQueryOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        gcp_conn_id,
        zendesk_conn_id,
        zendesk_service_type,
        zendesk_request_type,
        *args,
        **kwargs,
    ):
        super(ZendeskToBigQueryOperator, self).__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        # self.zendesk_conn_id = Variable.get("zendesk_conn")
        # self.zendesk_conn_id = zendesk_conn_id
        self.zendesk_service_type = zendesk_service_type
        self.zendesk_request_type = zendesk_request_type

    def execute(self, context):
        self.log.info(f"start execute...")
        try:
            # connections 에 넣을때는 아래처럼 쓴다.
            # base_hook = BaseHook("")
            # zendesk_conn = base_hook.get_connection(self.zendesk_conn_id)
            # zendesk_api_token = zendesk_conn.password
            zendesk_api_token = Variable.get("zendesk_conn")

            gcp_conn = GoogleCloudBaseHook(gcp_conn_id=self.gcp_conn_id)
            google_credentials = gcp_conn._get_credentials()

            # search request에서 스케쥴 간격을 1일로 지정하는 경우,
            # execution 시간은 실제 스케쥴 일자보다 1일 앞서게 됨
            # ZendeskToBigqueryLoader에서는 etl_start_time의 date만 추출하여 사용하는데,
            # 1일 전 날짜로 search request 요청하면 updated_at 기준으로 변경된 데이터를 빠짐없이 가져올 수 있음
            # execution_date 는 composer 2.2 부터 derprecate
            # execution_datetime = context.get("execution_date")
            execution_datetime = context.get("logical_date")
            etl_start_timestamp = int(execution_datetime.timestamp())
            etl_end_timestamp = int(execution_datetime.timestamp())  # 현재 사용하지 않음
            self.log.info(f"execute(): execution_date [{str(execution_datetime)}]")

            zendesk_to_bigquery_loader = ZendeskToBigqueryLoader(
                zendesk_api_token, google_credentials
            )
            zendesk_to_bigquery_loader.do_etl(
                self.zendesk_request_type,
                etl_start_timestamp,
                etl_end_timestamp,
                self.zendesk_service_type,
            )
        except Exception as e:
            self.log.exception(e)
            raise
