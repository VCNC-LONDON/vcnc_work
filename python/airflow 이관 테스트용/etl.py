from datetime import datetime, timedelta
from dependencies import slack_operator, add_description, utils
import os
from airflow import models
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
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
    "start_date": datetime(2022, 8, 24),
    "email": ["london@vcnc.co.kr"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "project_id": "kr-co-vcnc-tada",
}

schema_description = [
    {"name": "date_kr", "description": "일자"},
    {"name": "cnt", "description": "갯수"},
]

with models.DAG(
    dag_id="Etl_test",
    description="Etl_test",
    schedule_interval="* * * * *",
    default_args=default_args,
    catchup=False,
) as dag:

    task_done_checker = DummyOperator(
        task_id="task_done_checker",
        trigger_rule="one_success",
    )

    elt_task = BigQueryOperator(
        task_id=f"extract_firebase_app_data",
        sql="""
        SELECT 
            date_kr, 
            COUNT(ride_id) AS cnt 
        FROM `kr-co-vcnc-tada.tada_store.ride_base` 
        WHERE date_kr >= '2022-08-01'
        GROUP BY date_kr
        """,
        bigquery_conn_id="google_cloud_for_tada",
        use_legacy_sql=False,
        destination_dataset_table="kr-co-vcnc-tada.tada_temp_london.composer_etl_test",
        write_disposition="WRITE_TRUNCATE",
        on_failure_callback=task_fail_slack_alert,
    )

    add_description_task = PythonOperator(
        task_id="description_task",
        python_callable=add_description.update_schema_description,
        op_kwargs={
            "table_fullname": "kr-co-vcnc-tada.tada_temp_london.composer_etl_test",
            "schema_description": schema_description,
        },
        on_failure_callback=task_fail_slack_alert,
    )

    (elt_task >> add_description_task >> task_done_checker)
