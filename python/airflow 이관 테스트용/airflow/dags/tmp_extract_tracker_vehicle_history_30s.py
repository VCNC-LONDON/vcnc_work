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
    {"name": "vehicle_id", "description": "차량 ID"},
    {"name": "time_ms", "description": "Epoch Milliseconds. 1970-01-01 00:00:00.000 부터 몇 Milliseconds 가 흘렀는지."},
    {"name": "time_delta_ms", "description": "동일 차량 ID 의 직전 record 와 시간 간격. millsecond 단위."},
    {"name": "lng", "description": "lng"},
    {"name": "lat", "description": "lng"},
    {"name": "bearing", "description": "차량 방향. azimuth 정의대로 정북 기준이고, 시계방향이 (+) 고, -180.0 ~ +180.0 사이 값."},
    {"name": "distance_delta_meters", "description": "동일 차량 ID 의 직전 record 과 거리 간격. meter 단위."},
    {"name": "speed_meter_per_second", "description": "속력. meter/second 단위."},
    {"name": "driver_id", "description": "드라이버 ID"},
    {"name": "ride_id", "description": "라이드 ID"},
    {"name": "ride_status", "description": "라이드 status"},
    {"name": "date_kr", "description": "partitioning 을 위한 컬럼. time_ms 의 Asia/Seoul 날짜."},
]

execute_date = "{{ds}}"
execute_date_formatted = "{{ds_nodash}}"

with models.DAG(
    dag_id="Extract-tracker_vehicle_history_30s",
    description="extract_tracker_vehicle_history_30s",
    schedule_interval="0 */3 * * *", 
    default_args=default_args,
    catchup=False,
) as dag:

    extract_tracker_vehicle_history_30s_task = BigQueryOperator(
        task_id="extract_tracker_vehicle_history_30s",
        sql=utils.read_sql(
            os.path.join(os.environ["DAGS_FOLDER"], "sql/__tracker_vehicle_history_30s.sql")
        ).format(execute_date=execute_date), # airflow time zone 설정 테스트 후 세팅
        bigquery_conn_id="google_cloud_for_tada",
        use_legacy_sql=False,
        destination_dataset_table=f"kr-co-vcnc-tada.tada_temp_london.tracker_vehicle_history_30s${execute_date_formatted}",
        # destination_dataset_table=f"kr-co-vcnc-tada.tada_ext.tracker_vehicle_history_30s${execute_date}",
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "DAY", "field": "date_kr"},
        on_failure_callback=task_fail_slack_alert,
    )

    add_description_task = PythonOperator(
        task_id="description_task",
        python_callable=add_description.update_schema_description,
        op_kwargs={
            # "table_fullname": "kr-co-vcnc-tada.tada_ext.tracker_vehicle_history_30s",
            "table_fullname": "kr-co-vcnc-tada.tada_temp_london.tracker_vehicle_history_30s",
            "schema_description": schema_description,
        },
        on_failure_callback=task_fail_slack_alert,
    )

    extract_tracker_vehicle_history_30s_task >> add_description_task
