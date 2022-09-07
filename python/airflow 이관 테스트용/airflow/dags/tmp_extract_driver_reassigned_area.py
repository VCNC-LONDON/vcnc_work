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
    {"name": "uuid", "description": "log uuid"},
    {"name": "date_kr", "description": "date_kr"},
    {"name": "time_kr", "description": "재배치 수행 시각(KTC)"},
    {"name": "time_ms", "description": "재배치 수행 unix second"},
    {"name": "assigned_area_candidates", "description": "재배치 후보군"},
    {"name": "dispatching_driver_cnt", "description": "수신중 기사수"},
    {"name": "driver_id", "description": "driver_id"},
    {"name": "last_ride_id", "description": "마지막 운행(배차) ride id"},
    {"name": "region_block_candidates", "description": "재배치 블락킹 지역 정보"},
    {"name": "search_distance_meters", "description": "탐색 반경"},
    {"name": "selected_assigned_area_lat", "description": "선정된 재배치 위치 lat"},
    {"name": "selected_assigned_area_lng", "description": "선정된 재배치 위치 lng"},
    {"name": "selected_assigned_area_name", "description": "선정된 재배치 위치 이름"},
    {"name": "selected_region_block_id", "description": " "},
    {"name": "working_driver_cnt", "description": "근무중 기사수"},
]

execute_date = "{{ds}}"
execute_date_formatted = "{{ds_nodash}}"

with models.DAG(
    dag_id="Extract-driver_reassigned_area",
    description="extract_driver_reassigned_area",
    schedule_interval= "0 */3 * * *", # 매일 3시간마다
    default_args=default_args,
    catchup=False,
) as dag:

    extract_driver_reassigned_area_task = BigQueryOperator(
        task_id="extract_driver_reassigned_area",
        sql=utils.read_sql(
            os.path.join(os.environ["DAGS_FOLDER"], "sql/__driver_reassigned_area.sql")
        ).format(execute_date=execute_date),
        bigquery_conn_id="google_cloud_for_tada",
        use_legacy_sql=False,
        destination_dataset_table=f"kr-co-vcnc-tada.tada_temp_london.driver_reassigned_area${execute_date_formatted}",
        # destination_dataset_table=f"kr-co-vcnc-tada.tada_ext.driver_reassigned_area${execute_date}",
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "DAY", "field": "date_kr"},
        on_failure_callback=task_fail_slack_alert,
    )

    add_description_task = PythonOperator(
        task_id="description_task",
        python_callable=add_description.update_schema_description,
        op_kwargs={
            "table_fullname": "kr-co-vcnc-tada.tada_temp_london.driver_reassigned_area",
            # "table_fullname": "kr-co-vcnc-tada.tada_ext.driver_reassigned_area",
            "schema_description": schema_description,
        },
        on_failure_callback=task_fail_slack_alert,
    )

    extract_driver_reassigned_area_task >> add_description_task
