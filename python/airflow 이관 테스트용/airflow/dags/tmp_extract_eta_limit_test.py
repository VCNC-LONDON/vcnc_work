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
    {"name": "ts", "description": "log 발생시각(UTC)"},
    {"name": "ride_id", "description": "ride_id"},
    {"name": "match_context_created_at", "description": "match_context 생성시각"},
    {"name": "driver_id", "description": "driver_id"},
    {"name": "vehicle_id", "description": "vehicle_id"},
    {"name": "reason", "description": "reason"},
    {"name": "on_deprioritizing_crossroad", "description": " "},
    {"name": "is_schedule_end_at_coming", "description": " "},
    {"name": "idx", "description": " "},
    {"name": "estimated_roi", "description": " "},
    {"name": "eta_duration_ms", "description": "계산된 eta 시간 (ms)"},
    {"name": "eta_duration_ms_upper_limit", "description": "계산된 eta 시간 (ms) 한도"},
    {"name": "filtered_by_roi", "description": "roi 기반 필터링 여부"},
    {"name": "filtered_by_eta", "description": "eta 기반 필터링 여부"},
    {"name": "rejected_by_roi", "description": "roi 기반 거절 여부"},
    {"name": "rejected_by_eta", "description": "eta 기반 거절 여부"},
    {"name": "date_kr", "description": "date_kr(파티션용)"},
]

execute_date = "{{ds}}"
execute_date_formatted = "{{ds_nodash}}"

# eta_limit_test 는 eta_limit_test 와 eta_limit_test_is_null 이 있는데 후자는 앞선 테이블에서 reason is null 조건만 추가된 것
with models.DAG(
    dag_id="Extract-eta_limit_test",
    description="extract_eta_limit_test",
    schedule_interval="0 */3 * * *", # 매일 3시간마다,
    default_args=default_args,
    catchup=False,
) as dag:

    extract_eta_limit_test_task = BigQueryOperator(
        task_id="extract_eta_limit_test",
        sql=utils.read_sql(
            os.path.join(os.environ["DAGS_FOLDER"], "sql/__eta_limit_test.sql")
        ).format(execute_date=execute_date), # airflow time zone 설정 테스트 후 세팅
        bigquery_conn_id="google_cloud_for_tada",
        use_legacy_sql=False,
        destination_dataset_table=f"kr-co-vcnc-tada.tada_temp_london.eta_limit_test${execute_date_formatted}",
        # destination_dataset_table=f"kr-co-vcnc-tada.tada_ext.eta_limit_test${execute_date}",
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "DAY", "field": "date_kr"},
        on_failure_callback=task_fail_slack_alert,
    )

    add_description_task = PythonOperator(
        task_id="description_task",
        python_callable=add_description.update_schema_description,
        op_kwargs={
            "table_fullname": "kr-co-vcnc-tada.tada_temp_london.eta_limit_test",
            # "table_fullname": "kr-co-vcnc-tada.tada_ext.eta_limit_test",
            "schema_description": schema_description,
        },
        on_failure_callback=task_fail_slack_alert,
    )

    extract_eta_limit_test_task >> add_description_task
