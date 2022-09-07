from datetime import datetime, timedelta
from dependencies import slack_operator, add_description, utils
import os
from airflow import models
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

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
    "start_date": datetime(2022, 7, 7),
    "email": ["london@vcnc.co.kr"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "project_id": "kr-co-vcnc-tada",
}

schema_description = [
    {"name": "date_kr", "description": "일자"},
    {"name": "user_id", "description": "user_id"},
    {
        "name": "user_pseudo_id",
        "description": "user_pseudo_id. user_id가 NULL일 경우 보조적으로 활용 가능한 컬럼",
    },
    {"name": "event_name", "description": "event_name"},
    {"name": "event_datetime", "description": "event_datetime(한국 시간)"},
    {"name": "first_touch_datetime", "description": "해당 유저의 첫 사용 시점(한국 시간)"},
    {"name": "platform", "description": "디바이스의 플랫폼(ANDROID, IOS)"},
    {"name": "mobile_brand_name", "description": "디바이스의 브랜드명. 예 : Samsung, Apple"},
    {"name": "mobile_model_name", "description": "디바이스의 기기명. 예 : SM-G973"},
    {
        "name": "advertising_id",
        "description": "advertising id로 같은 advertising_id는 동일한 디바이스(단, 디바이스에서 초기화 가능)",
    },
    {"name": "device_language", "description": "device의 언어"},
    {"name": "continent", "description": "firebase에서 제공한 대륙 정보"},
    {"name": "country", "description": "firebase에서 제공한 국가 정보"},
    {"name": "region", "description": "firebase에서 제공한 지역 정"},
    {"name": "city", "description": "firebase에서 제공한 도시 정보"},
    {"name": "app_version", "description": "app version"},
    {
        "name": "app_id",
        "description": "app_id로 kr.co.vcnc.tada(타다 앱)와 kr.co.vcnc.tada.driver(드라이버 앱)이 있음",
    },
    {"name": "event_params", "description": "event의 파라미터"},
    {"name": "user_property", "description": "user_property 항목"},
]

yesterday = "{{ macros.ds_add(ds, -0) }}"
yesterday_suffix = "{{ macros.ds_format(macros.ds_add(ds, -0), '%Y-%m-%d', '%Y%m%d') }}"

with models.DAG(
    dag_id="Extract-firebase_app_log_v2",
    description="extract_firebase_app_logs",
    schedule_interval="0 1 * * *",
    default_args=default_args,
    catchup=False,
) as dag:

    check_analytics_table_update = BigQueryTableExistenceSensor(
        task_id="check_analytics_table_update",
        project_id="kr-co-vcnc-tada",
        dataset_id="analytics_181161192",
        table_id=f"events_{yesterday_suffix}",
        gcp_conn_id="google_cloud_for_tada",
    )

    task_done_checker = DummyOperator(
        task_id="task_done_checker",
        trigger_rule="one_success",
    )

    extract_firebase_app_task = BigQueryOperator(
        task_id=f"extract_firebase_app_data",
        sql=utils.read_sql(
            os.path.join(os.environ["DAGS_FOLDER"], "sql/firebase_app_event.sql")
        ).format(execute_date=yesterday_suffix),
        bigquery_conn_id="google_cloud_for_tada",
        use_legacy_sql=False,
        destination_dataset_table=f"kr-co-vcnc-tada.tada_ext.firebase_app_event${yesterday_suffix}",
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "DAY", "field": "date_kr"},
        on_failure_callback=task_fail_slack_alert,
    )

    add_description_task = PythonOperator(
        task_id="description_task",
        python_callable=add_description.update_schema_description,
        op_kwargs={
            "table_fullname": "kr-co-vcnc-tada.tada_ext.firebase_app_event",
            "schema_description": schema_description,
        },
        on_failure_callback=task_fail_slack_alert,
    )

    (
        check_analytics_table_update
        >> task_done_checker
        >> extract_firebase_app_task
        >> add_description_task
    )
