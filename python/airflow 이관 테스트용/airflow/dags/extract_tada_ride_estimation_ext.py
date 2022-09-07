from datetime import datetime, timedelta
import os
from airflow import models
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from dependencies import slack_operator, add_description, utils

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
        "description": "로그 타입 (USER_ESTIMATE_RIDE, DISPATCHER_ALTERNATIVE_RIDE_SUGGESTION)",
    },
    {"name": "time_ms", "description": "로그 시각 (millisecond)"},
    {"name": "time_kr", "description": "로그 시각 (한국 datetime)"},
    {
        "name": "estimation_uuid",
        "description": "estimation uuid (ride_request 와의 join key)",
    },
    {"name": "rider_id", "description": "유저 ID"},
    {"name": "user_agent", "description": "유저 단말기 정보"},
    {"name": "appsflyer_id", "description": "유저 appsflyer ID"},
    {"name": "ga_id", "description": "유저 GA ID"},
    {"name": "fa_id", "description": "유저 FA ID"},
    {"name": "estimation_ride_type", "description": "가격 확인 타입 (넛지의 경우 남지 않음)"},
    {"name": "alternative_ride_types_list", "description": "넛지로 추천해주는 다른 타입"},
    {
        "name": "lite_unavailable_by_no_nearby_vehicle",
        "description": "라이트 주변차 없음 (21.02.10 부터 적재)",
    },
    {
        "name": "unavailable_by_no_nearby_vehicle_list",
        "description": "주변차 없음 (21.11.17 부터 적재)",
    },
    {"name": "estimation_success", "description": "estimation 성공여부"},
    {"name": "origin_lat", "description": "출발지 위도"},
    {"name": "origin_lng", "description": "출발지 경도"},
    {"name": "origin_name", "description": "출발지 이름"},
    {"name": "origin_address", "description": "출발지 주소"},
    {"name": "destination_lat", "description": "도착지 위도"},
    {"name": "destination_lng", "description": "도착지 경도"},
    {"name": "destination_name", "description": "도착지 이름"},
    {"name": "destination_address", "description": "도착지 주소"},
    {"name": "route_uuid", "description": "경로 uuid"},
    {"name": "distance_meters", "description": "경로 예상거리"},
    {"name": "duration_seconds", "description": "경로 예상시간"},
    {"name": "min_cost", "description": "최소 가격"},
    {"name": "max_cost", "description": "최고 가격"},
    {"name": "surge_percentage", "description": "서지율"},
    {"name": "surge_policy_id", "description": "서지 정책 ID"},
    {"name": "price_elasticity_test_percentage", "description": "가격탄력성 실험용 서지율"},
    {"name": "coupon_id", "description": "적용 쿠폰 ID"},
    {"name": "date_kr", "description": "파티션 date_kr (timeMs 기준)"},
]

date_suffix = "{{ macros.ds_add(ds, -0) }}"
date_suffix_formatted = (
    "{{ macros.ds_format(macros.ds_add(ds, -0), '%Y-%m-%d', '%Y%m%d') }}"
)

with models.DAG(
    dag_id="Extract-ride-estimation-ext",
    description="ride-estimation-ext",
    schedule_interval="10 * * * *",  # every 1 hour at 10 minute
    catchup=False,
    default_args=default_args,
) as dag:

    ride_estimation_task = BigQueryOperator(
        dag=dag,
        bigquery_conn_id="google_cloud_for_tada",
        task_id=f"extract_ride_estimation_ext",
        sql=utils.read_sql(
            os.path.join(os.environ["DAGS_FOLDER"], "sql/ride_estimation_ext.sql")
        ).format(target_date=date_suffix),
        use_legacy_sql=False,
        destination_dataset_table=f"kr-co-vcnc-tada.tada_ext.ride_estimation_ext${date_suffix_formatted}",
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "DAY", "field": "date_kr"},
        on_failure_callback=task_fail_slack_alert,
    )

    add_description_task = PythonOperator(
        task_id="description_task",
        python_callable=add_description.update_schema_description,
        op_kwargs={
            "table_fullname": "kr-co-vcnc-tada.tada_ext.ride_estimation_ext",
            "schema_description": schema_description,
        },
        dag=dag,
    )

    ride_estimation_task >> add_description_task
