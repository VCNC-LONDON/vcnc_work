from datetime import datetime, timedelta
from airflow import models
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from dependencies import slack_operator, add_description, utils

import os


SLACK_CONN_ID = "slack"


def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed.
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )
    failed_alert = slack_operator.SlackWebhookOperator(
        task_id="slack_test",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="tada-airflow",
        dag=dag,
    )
    return failed_alert.execute(context=context)


default_args = {
    "owner": "kristoff",
    "depends_on_past": True,
    "start_date": datetime(2021, 12, 13),
    "email": ["kristoff@vcnc.co.kr"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "project_id": "kr-co-vcnc-tada",
}

schema_description = [
    {"name": "log_uuid", "description": "로그 uuid"},
    {
        "name": "log_type",
        "description": "로그 타입 (USER_REQUEST_RIDE, USER_REQUEST_ALTERNATIVE_RIDE)",
    },
    {"name": "time_ms", "description": "로그 시각 (millisecond)"},
    {"name": "time_kr", "description": "로그 시각 (한국 datetime)"},
    {
        "name": "estimation_uuid",
        "description": "estimation uuid (ride_request_ext 와의 join key)",
    },
    {"name": "rider_id", "description": "유저 ID"},
    {"name": "user_agent", "description": "유저 단말기 정보"},
    {"name": "appsflyer_id", "description": "유저 appsflyer ID"},
    {"name": "ga_id", "description": "유저 GA ID"},
    {"name": "fa_id", "description": "유저 FA ID"},
    {"name": "ride_id", "description": "호출 ID"},
    {"name": "ride_type", "description": "호출 타입 (LITE, PREMIUM, NXT, NEAR_TAXI)"},
    {"name": "ride_status", "description": "호출 status"},
    {"name": "ride_created_at_ms", "description": "호출시각 (millisecond)"},
    {"name": "ride_created_at_kr", "description": "호출시간 (한국 datetime)"},
    {"name": "vehicle_id", "description": "차량 ID"},
    {"name": "driver_id", "description": "드라이버 ID"},
    {"name": "origin_lat", "description": "출발지 위도"},
    {"name": "origin_lng", "description": "출발지 경도"},
    {"name": "origin_name", "description": "출발지 이름"},
    {"name": "origin_address", "description": "출발지 주소"},
    {"name": "destination_lat", "description": "도착지 위도"},
    {"name": "destination_lng", "description": "도착지 경도"},
    {"name": "destination_name", "description": "도착지 이름"},
    {"name": "destination_address", "description": "도착지 주소"},
    {"name": "origin_source", "description": "출발지 출처"},
    {"name": "destination_source", "description": "도착치 출처"},
    {"name": "route_uuid", "description": "경로 uuid"},
    {"name": "distance_meters", "description": "경로 예상거리"},
    {"name": "duration_seconds", "description": "경로 예상시간"},
    {
        "name": "suggestion_type",
        "description": "넛지 사유 (ON_DISPATCH_FAILED, ON_RIDE_TIMEOUT)",
    },
    {"name": "near_ride_cause", "description": "가까운타다 매칭 사유"},
    {"name": "determined_ride_type", "description": "가까운타다 매칭차량 타입"},
    {"name": "original_ride_id", "description": "넛지 전 호출 ID"},
    {"name": "min_cost", "description": "최소 가격"},
    {"name": "max_cost", "description": "최고 가격"},
    {"name": "coupon_id", "description": "쿠폰 ID"},
    {"name": "date_kr", "description": "파티션 date_kr (timeMs 기준)"},
]

date_suffix = "{{ macros.ds_add(ds, -0) }}"
date_suffix_formatted = (
    "{{ macros.ds_format(macros.ds_add(ds, -0), '%Y-%m-%d', '%Y%m%d') }}"
)

with models.DAG(
    dag_id="Extract-ride-request-ext",
    description="ride-request-ext",
    schedule_interval="10 * * * *",  # every 1 hour at 10 minute
    catchup=False,
    default_args=default_args,
) as dag:

    ride_request_task = BigQueryOperator(
        dag=dag,
        bigquery_conn_id="google_cloud_for_tada",
        task_id=f"extract_ride_request_ext",
        sql=utils.read_sql(
            os.path.join(os.environ["DAGS_FOLDER"], "sql/ride_request_ext.sql")
        ).format(target_date=date_suffix),
        use_legacy_sql=False,
        destination_dataset_table=f"kr-co-vcnc-tada.tada_ext.ride_request_ext${date_suffix_formatted}",
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "DAY", "field": "date_kr"},
        on_failure_callback=task_fail_slack_alert,
    )

    add_description_task = PythonOperator(
        task_id="description_task",
        python_callable=add_description.update_schema_description,
        op_kwargs={
            "table_fullname": "kr-co-vcnc-tada.tada_ext.ride_request_ext",
            "schema_description": schema_description,
        },
        dag=dag,
    )

    ride_request_task >> add_description_task
