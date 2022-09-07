from datetime import datetime, timedelta
from airflow import models
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from dependencies import slack_operator, add_description


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
    "owner": "kyle",
    "depends_on_past": False,
    "start_date": datetime(2021, 6, 23),
    "email": ["kyle@socar.kr"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "project_id": "kr-co-vcnc-tada",
}

server_log_type = "SERVER_INACTIVATE_USER"

execution_date = "{{ macros.ds_add(ds, -0) }}"

execution_date_suffix = (
    "{{ macros.ds_format(macros.ds_add(ds, -0), '%Y-%m-%d', '%Y%m%d') }}"
)


extract_query = """
SELECT 
  userInactivationLog.userId AS user_id,
  DATETIME(TIMESTAMP_MILLIS(userInactivationLog.inactivatedAt), 'Asia/Seoul') AS datetime_kr,
  type,
  uuid,
  date_kr
FROM `kr-co-vcnc-tada.tada.server_log_parquet`
WHERE date_kr = "{execution_date}"
AND type = "{server_log_type}"
"""

schema_description = [
    {"name": "user_id", "description": "user_id"},
    {"name": "datetime_kr", "description": "비활성화한 DATETIME"},
    {"name": "type", "description": f"서버 로그 타입({server_log_type})"},
    {"name": "uuid", "description": "UUID"},
    {"name": "date_kr", "description": "일자(파티션용)"},
]


with models.DAG(
    dag_id=f"Extract-server-log-{server_log_type}",
    description=f"extract-server-log-{server_log_type}",
    schedule_interval="0 * * * *",
    catchup=False,
    default_args=default_args,
) as dag:

    extract_server_log = BigQueryOperator(
        dag=dag,
        bigquery_conn_id="google_cloud_for_tada",
        task_id=f"extract-server-log-{server_log_type}",
        sql=extract_query.format(
            execution_date=execution_date, server_log_type=server_log_type
        ),
        use_legacy_sql=False,
        destination_dataset_table=f"kr-co-vcnc-tada.tada_server_log.{server_log_type}${execution_date_suffix}",
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "DAY", "field": "date_kr"},
        on_failure_callback=task_fail_slack_alert,
    )

    add_description_task = PythonOperator(
        dag=dag,
        task_id="description_task",
        python_callable=add_description.update_schema_description,
        op_kwargs={
            "table_fullname": f"kr-co-vcnc-tada.tada_server_log.{server_log_type}",
            "schema_description": schema_description,
        },
        on_failure_callback=task_fail_slack_alert,
    )

    extract_server_log >> add_description_task
