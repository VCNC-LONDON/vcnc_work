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
    {"name": "date_kr", "description": "date_kr"},
    {"name": "ride_id", "description": "ride_id"},
    {"name": "created_at_kr", "description": "ride 생성 시간"},
    {"name": "model_created_at_kr", "description": "eta 모델 생성 시간"},
    {"name": "output_eta", "description": "계산된 eta"},
    {"name": "input_eta", "description": "입력된 eta"},
    {"name": "input_origin_gu", "description": "입력된 출발지 위치(구)"},
    {"name": "input_driver_gu", "description": "입력된 드라이버 위치(구)"},
    {"name": "input_driver_id", "description": "입력된 driver_id)"},
]

execute_date = "{{ds}}"
execute_date_formatted = "{{ds_nodash}}"

with models.DAG(
    dag_id="Extract-ride_eta_adjust",
    description="extract_ride_eta_adjust",
    schedule_interval="0 */3 * * *", #매일 3시간마다
    default_args=default_args,
    catchup=False,
) as dag:

    extract_ride_eta_adjust_task = BigQueryOperator(
        task_id="extract_ride_eta_adjust",
        sql=utils.read_sql(
            os.path.join(os.environ["DAGS_FOLDER"], "sql/__ride_eta_adjust.sql")
        ).format(execute_date=execute_date), 
        bigquery_conn_id="google_cloud_for_tada",
        use_legacy_sql=False,
        destination_dataset_table=f"kr-co-vcnc-tada.tada_temp_london.ride_eta_adjust${execute_date_formatted}",
        # destination_dataset_table=f"kr-co-vcnc-tada.tada_ext.rid_eta_adjust${execute_date}",
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "DAY", "field": "date_kr"},
        on_failure_callback=task_fail_slack_alert,
    )

    add_description_task = PythonOperator(
        task_id="description_task",
        python_callable=add_description.update_schema_description,
        op_kwargs={
            "table_fullname": "kr-co-vcnc-tada.tada_temp_london.ride_eta_adjust",
            # "table_fullname": "kr-co-vcnc-tada.tada_ext.ride_eta_adjust",
            "schema_description": schema_description,
        },
        on_failure_callback=task_fail_slack_alert,
    )

    extract_ride_eta_adjust_task >> add_description_task
