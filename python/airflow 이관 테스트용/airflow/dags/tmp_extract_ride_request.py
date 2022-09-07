from datetime import datetime
from dependencies import slack_operator, add_description, utils
import os
from airflow import models
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator

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
    "owner": "london",
    "depends_on_past": False,
    "start_date": datetime(2022, 9, 6),
    "email": ["london@vcnc.co.kr"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "project_id": "kr-co-vcnc-tada",
}

schema_description = [
    {"name": "uuid", "description": "log_uuid"},
    {"name": "view_uuid", "description": "view_uuid"},
    {"name": "type", "description": "log_name"},
    {"name": "time_ms", "description": "log 생성 시간(unix ms)"},
    {"name": "time_kr", "description": "log 생성 시각(KTC)"},
    {"name": "user_id", "description": "user_id"},
    {"name": "user_agent", "description": "user_agent"},
    {"name": "appsflyer_id", "description": "appsflyer 에서 생성된 third party id"},
    {"name": "distance_meters", "description": "예상 이동거리(m)"},
    {"name": "duration_seconds", "description": "예상 이동시간(초)"},
    {"name": "max_cost", "description": "예상 최대 금액"},
    {"name": "min_cost", "description": "예상 최소 금액"},
    {"name": "created_at_ms", "description": "ride 생성 시간(unix ms)"},
    {"name": "created_at_kr", "description": "ride 생성 시각"},
    {"name": "ride_id", "description": "ride_id"},
    {"name": "ride_type", "description": "ride_type(NXT, PREMIUM, LITE)"},
    {"name": "vehicle_id", "description": "vehicle_id"},
    {"name": "origin_address", "description": "출발지 주소"},
    {"name": "origin_name", "description": "출발지 건물명"},
    {"name": "origin_lat", "description": "출발지 lat"},
    {"name": "origin_lng", "description": "출발지 lng"},
    {"name": "destination_address", "description": "도착지 주소"},
    {"name": "destination_name", "description": "도착지 이름"},
    {"name": "destination_lat", "description": "도착지 lat"},
    {"name": "destination_lng", "description": "도착지 lng"},
    {"name": "destination_eta_ms", "description": "도착지 도착 예상시간(eta)(unix ms)"},
    {"name": "origin_source", "description": "출발지 소스(GEOCODE,HISTORY,PLACE,CURRENT,UNSPECIFIED)"},
    {"name": "destination_source", "description": "도착지 소스(GEOCODE,HISTORY,HISTORY_PATH,PLACE,FAVORITE,UNSPECIFIED)"},
    {"name": "origin_eta_ms", "description": "보정된 출발지 도착 예상시간(eta)(unix ms)"},
    {"name": "raw_origin_eta_ms", "description": "출발지 도착 예상시간(eta)(unix ms)"},
    {"name": "eta_model_created_at_ms", "description": "적용된 eta model 생성시각"},
    {"name": "price_rating", "description": "price_rating"},
    {"name": "coupon_id", "description": "적용된 coupon_id"},
    {"name": "default_coupon_id", "description": "적용된 기본 coupon_id"},
    {"name": "payment_method_id", "description": "payment_method 테이블의 id"},
    {"name": "review_rating_by_user", "description": "유저의 리뷰 점수"},
    {"name": "route_distance_meters", "description": "실 이동거리"},
    {"name": "route_uuid", "description": "경로 ID"},
    {"name": "status", "description": "ride 의 status"},
    {"name": "app_reviewed", "description": "app 평가 여부"},
    {"name": "satisfied", "description": " "},
    {"name": "date_kr", "description": "date_kr (파티션)"},
]

execute_date = "{{ds}}"
execute_date_formatted = "{{ds_nodash}}"

with models.DAG(
    dag_id="Extract-ride_request",
    description="extract_ride_request",
    schedule_interval="0 */3 * * *",
    default_args=default_args,
    catchup=False,
) as dag:

    extract_ride_request_task = BigQueryOperator(
        task_id="extract_ride_request",
        sql=utils.read_sql(
            os.path.join(os.environ["DAGS_FOLDER"], "sql/__ride_request.sql")
        ).format(execute_date=execute_date), # airflow time zone 설정 테스트 후 세팅
        bigquery_conn_id="google_cloud_for_tada",
        use_legacy_sql=False,
        destination_dataset_table=f"kr-co-vcnc-tada.tada_temp_london.ride_request${execute_date_formatted}",
        # destination_dataset_table=f"kr-co-vcnc-tada.tada_ext.ride_request${execute_date}",
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "DAY", "field": "date_kr"},
        on_failure_callback=task_fail_slack_alert,
    )

    add_description_task = PythonOperator(
        task_id="description_task",
        python_callable=add_description.update_schema_description,
        op_kwargs={
            "table_fullname": "kr-co-vcnc-tada.tada_temp_london.ride_request",
            # "table_fullname": "kr-co-vcnc-tada.tada_ext.ride_request",
            "schema_description": schema_description,
        },
        on_failure_callback=task_fail_slack_alert,
    )

    extract_ride_request_task >> add_description_task
