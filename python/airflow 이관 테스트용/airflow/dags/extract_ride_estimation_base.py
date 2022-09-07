from datetime import datetime, timedelta
import os

from airflow import models
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
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
    "owner": "london",
    "depends_on_past": True,
    "start_date": datetime(2022, 4, 1),
    "email": ["london@vcnc.co.kr"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "project_id": "kr-co-vcnc-tada",
}

schema_description = [
    {"name": "estimation_log_uuid", "description": "호출뷰 로그 uuid"},
    {"name": "date_kr", "description": "호출뷰에 진입한 시점 기준의 일자 (파티션 컬럼)"},
    {"name": "estimate_time_kr", "description": "호출뷰에 진입한 시점"},
    {"name": "rider_id", "description": "라이더 아이디"},
    {"name": "origin_address", "description": "호출뷰에서 입력한 출발지 주소"},
    {"name": "origin_lat", "description": "호출뷰 출발지 위도"},
    {"name": "origin_lng", "description": "호출뷰 출발지 경도"},
    {"name": "origin_h3_l7", "description": "호출뷰 출발지 H3 L7"},
    {"name": "origin_h3_l9", "description": "호출뷰 출발지 H3 L9"},
    {"name": "origin_region", "description": "호출뷰 출발지 지역"},
    {"name": "destination_address", "description": "호출뷰에서 입력한 목적지 주소"},
    {"name": "destination_lat", "description": "호출뷰 목적지 위도"},
    {"name": "destination_lng", "description": "호출뷰 목적지 경도"},
    {"name": "destination_h3_l7", "description": "호출뷰 목적지 H3 L7"},
    {"name": "destination_h3_l9", "description": "호출뷰 목적지 H3 L9"},
    {"name": "destination_region", "description": "호출뷰 목적지 지역"},
    {"name": "distance_meters", "description": "예상 이동 거리"},
    {"name": "duration_seconds", "description": "예상 이동 시간"},
    {"name": "nxt_cost", "description": "넥스트 요금"},
    {"name": "lite_cost", "description": "라이트 요금"},
    {"name": "plus_cost", "description": "플러스 요금"},
    {"name": "nxt_surge_percentage", "description": "넥스트 서지"},
    {"name": "lite_surge_percentage", "description": "라이트 서지"},
    {"name": "plus_surge_percentage", "description": "플러스 서지"},
    {
        "name": "is_nxt_nearby_vehicle",
        "description": "넥스트 주변차 유무 (True : 주변차 있음, False : 모든 차량 운행 중)",
    },
    {
        "name": "is_lite_nearby_vehicle",
        "description": "라이트 주변차 유무 (True : 주변차 있음, False : 모든 차량 운행 중)",
    },
    {
        "name": "is_plus_nearby_vehicle",
        "description": "플러스 주변차 유무 (True : 주변차 있음, False : 모든 차량 운행 중)",
    },
    {"name": "request_time_kr", "description": "호출 시점"},
    {"name": "ride_id", "description": "라이드 아이디"},
    {
        "name": "original_ride_id",
        "description": "이전 라이드 아이디 (넛지로 호출이 이루어진 경우 이전 라이드 아이디가 존재합니다.)",
    },
    {
        "name": "call_view_type",
        "description": "호출 화면 타입 (21/01/05부터 적재) (LITE, PREMIUM, NXT, NEAR_TAXI, NXT_NUDGE, LITE_NUDGE, PREMIUM_NUDGE, NEAR_TAXI_NUDGE)",
    },
    {
        "name": "call_service_type",
        "description": "호출 서비스 타입 (LITE, PREMIUM, NXT, NEAR_TAXI)",
    },
    {"name": "type", "description": "ride 테이블의 type (LITE, PREMIUM, NXT)"},
    {
        "name": "suggestion_type",
        "description": "넛지 발생 상황: MATCH_DELAY(매칭 지연시), MATCH_FAIL(매칭 실패시)",
    },
    {
        "name": "status",
        "description": "호출 상태 (CANCELED, DROPPED_OFF, PENDING, ACCEPTED, ARRIVED, PICKED_UP)",
    },
    {
        "name": "cancellation_cause",
        "description": "취소 원인 (DISPATCH_TIMEOUT / DRIVER_CANCELLED / RIDER_CANCELLED)",
    },
    {
        "name": "is_pure_session",
        "description": "순수수요세션 여부 (수요세션 동안 LITE, PREMIUM, NXT, NEAR_TAXI 중 하나의 타입으로만 호출했는지)",
    },
    {"name": "is_valid_5_last", "description": "유효 호출 기준 부합 여부"},
    {"name": "is_valid_estimate_view", "description": "유효 호출 뷰 기준 부합 여부"},
    {"name": "estimate_coupon_id", "description": "호출뷰 단계에서 적용한 쿠폰 아이디"},
    {"name": "request_coupon_id", "description": "호출 단계에서 적용한 쿠폰 아이디"},
]

date_suffix = ["{{ macros.ds_add(ds, +1) }}", "{{ macros.ds_add(ds, -0) }}"]
date_suffix_formatted = [
    "{{ macros.ds_format(macros.ds_add(ds, +1), '%Y-%m-%d', '%Y%m%d') }}",
    "{{ macros.ds_format(macros.ds_add(ds, -0), '%Y-%m-%d', '%Y%m%d') }}",
]

rule = ["today", "before_one_day"]

with models.DAG(
    dag_id="Extract-ride-estimation-base",
    description="ride-estimation-base",
    schedule_interval="30 * * * *",
    catchup=False,
    default_args=default_args,
) as dag:
    combine_task = DummyOperator(
        task_id="combine_tasks",
        trigger_rule="all_success",
        dag=dag,
    )

    for i in range(0, 2):
        ride_funnel_task = BigQueryOperator(
            dag=dag,
            bigquery_conn_id="google_cloud_for_tada",
            task_id=f"extract_ride_estimation_base_{rule[i]}",
            sql=utils.read_sql(
                os.path.join(os.environ["DAGS_FOLDER"], "sql/ride_estimation_base.sql")
            ).format(target_date=date_suffix[i]),
            use_legacy_sql=False,
            destination_dataset_table=f"kr-co-vcnc-tada.tada_store.ride_estimation_base${date_suffix_formatted[i]}",
            write_disposition="WRITE_TRUNCATE",
            time_partitioning={"type": "DAY", "field": "date_kr"},
            on_failure_callback=task_fail_slack_alert,
        )

        ride_funnel_task >> combine_task

    add_description_task = PythonOperator(
        task_id="description_task",
        python_callable=add_description.update_schema_description,
        op_kwargs={
            "table_fullname": "kr-co-vcnc-tada.tada_store.ride_estimation_base",
            "schema_description": schema_description,
        },
        dag=dag,
    )

    combine_task >> add_description_task
