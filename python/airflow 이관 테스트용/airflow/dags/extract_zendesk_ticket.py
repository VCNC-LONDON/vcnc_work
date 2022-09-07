from datetime import datetime, timedelta

from airflow.models import DAG

from dependencies.zendesk.zendesk_ticket import ZendeskTicket
from dependencies.zendesk.zendesk_to_bigquery_operator import ZendeskToBigQueryOperator

default_args = {
    "owner": "london",  # socar 의 thomas 가 작성하신 dag을 이관하면서 owner 를 수정합니다.
    "depends_on_past": False,
    "email": ["london@vcnc.co.kr"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "gcp_conn_id": "google_cloud_default",
    "zendesk_conn_id": "zendesk_conn",
}

with DAG(
    dag_id="Extract_zendesk_ticket",
    description="load Zendesk ticket data to BigQuery",
    start_date=datetime(2021, 2, 23),
    schedule_interval="5 15,23,8 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:

    zendesk_ticket = ZendeskTicket()
    zendesk_ticket_operator = ZendeskToBigQueryOperator(
        task_id="zendesk_ticket_task",
        zendesk_service_type=zendesk_ticket,
        zendesk_request_type="search",
    )
