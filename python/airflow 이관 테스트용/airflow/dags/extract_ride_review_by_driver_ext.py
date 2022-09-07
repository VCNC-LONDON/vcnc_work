import os
from datetime import datetime, timedelta
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
    "owner": "london",
    "depends_on_past": False,
    "start_date": datetime(2022, 4, 18),
    "email": ["london@vcnc.co.kr"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "project_id": "kr-co-vcnc-tada",
}

with models.DAG(
    dag_id="Extract-ride-review-by-driver-ext",
    description="Extract ride_review_by_driver_ext",
    schedule_interval=timedelta(hours=1),
    catchup=False,
    default_args=default_args,
) as dag:

    query_task = BigQueryOperator(
        dag=dag,
        bigquery_conn_id="google_cloud_for_tada",
        task_id="extract_ride_review_by_driver_ext",
        sql=utils.read_sql(
            os.path.join(os.environ["DAGS_FOLDER"], "sql/ride_review_by_driver_ext.sql")
        ),
        use_legacy_sql=False,
        destination_dataset_table="kr-co-vcnc-tada.tada_ext.ride_review_by_driver_ext",
        write_disposition="WRITE_TRUNCATE",
        on_failure_callback=task_fail_slack_alert,
    )

    query_task
