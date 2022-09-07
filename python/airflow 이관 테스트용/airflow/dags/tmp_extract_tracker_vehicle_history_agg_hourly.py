from datetime import datetime
from dependencies import slack_operator, utils
import os
from airflow import models
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

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
    "retries": 2,
    "retry_exponential_backoff": True,
    "project_id": "kr-co-vcnc-tada",
}

execute_date = "{{ds}}"
execute_date_formatted = "{{ds_nodash}}"

with models.DAG(
    dag_id="Extract-tracker_vehicle_history_agg_hourly",
    description="시간대별 서비스 타입별 이동거리를 계산하는 elt",
    schedule_interval="0 */3 * * *",
    default_args=default_args,
    catchup=False,
) as dag:

    extract_tracker_vehicle_history_agg_hourly = BigQueryOperator(
        task_id = "extract_tracker_vehicle_history_agg_hourly",
        sql = utils.read_sql(
            os.path.join(os.environ["DAGS_FOLDER"], "sql/__tracker_vehicle_history_agg_hourly.sql")
        ).format(execute_date=execute_date),
        bigquery_conn_id="google_cloud_for_tada",
        use_legacy_sql=False,
        destination_dataset_table=f"kr-co-vcnc-tada.tada_temp_london.tracker_vehicle_history_agg_hourly${execute_date_formatted}",
        # destination_dataset_table=f"kr-co-vcnc-tada.tada_ext.tracker_vehicle_history_agg_hourly${execute_date}",
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "DAY", "field": "date_kr"},
        on_failure_callback=task_fail_slack_alert,
    )
    
    extract_tracker_vehicle_history_agg_hourly