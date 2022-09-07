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
    "owner": "kristoff",
    "depends_on_past": True,
    "start_date": datetime(2021, 12, 14),
    "email": ["kristoff@vcnc.co.kr"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "project_id": "kr-co-vcnc-tada",
}

schema_description = [
    {"name": "date_kr", "description": "파티션 date_kr"},
    {"name": "origin_region", "description": "서울, 부산, 성남, 그외"},
    {"name": "type", "description": "LITE, PREMIUM, NXT"},
    {"name": "timeout_before_dispatch", "description": "주변 차량을 1분간 찾았지만 1대도 없어서 호출취소"},
    {
        "name": "rider_cancelled_before_dispatch",
        "description": "1분간 주변 차량을 찾는 도중에 유저가 취소",
    },
    {
        "name": "rider_cancelled_while_dispatching",
        "description": "드라이버에게 주어진 수락대기시간 동안 유저가 취소",
    },
    {
        "name": "rider_cancelled_after_accepted",
        "description": "드라이버가 콜수락 후 유저에게 이동중 유저가 취소",
    },
    {"name": "dropped_off", "description": "하차완료"},
    {"name": "driver_not_accept", "description": "드라이버에게 주어진 수락대기시간 동안 드라이버가 수락하지 않음"},
    {
        "name": "driver_cancelled_while_dispatching",
        "description": "드라이버에게 수락여부를 물었을 때 드라이버가 거절",
    },
    {
        "name": "driver_cancelled_after_accepted",
        "description": "콜 수락후 유저에게 이동중 드라이버가 취소",
    },
    {
        "name": "driver_not_accept_dispatch",
        "description": "드라이버에게 주어진 수락대기시간 동안 드라이버가 수락하지 않음 (dispatch_id 기준)",
    },
    {
        "name": "driver_cancelled_while_dispatching_dispatch",
        "description": "드라이버에게 주어진 수락대기시간 동안 드라이버가 거절(dispatch_id 기준)",
    },
]

date_suffix = ["{{ macros.ds_add(ds, +1) }}", "{{ macros.ds_add(ds, -0) }}"]
date_suffix_formatted = [
    "{{ macros.ds_format(macros.ds_add(ds, +1), '%Y-%m-%d', '%Y%m%d') }}",
    "{{ macros.ds_format(macros.ds_add(ds, -0), '%Y-%m-%d', '%Y%m%d') }}",
]

rule = ["today", "before_one_day"]

with models.DAG(
    dag_id="Extract-ride-funnel-metric",
    description="ride-funnel-metric",
    schedule_interval="20 * * * *",
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
            task_id=f"extract_ride_funnel_{rule[i]}",
            sql=utils.read_sql(
                os.path.join(os.environ["DAGS_FOLDER"], "sql/ride_funnel_metric.sql")
            ).format(target_date=date_suffix[i]),
            use_legacy_sql=False,
            destination_dataset_table=f"kr-co-vcnc-tada.tada_store.ride_funnel_metric${date_suffix_formatted[i]}",
            write_disposition="WRITE_TRUNCATE",
            time_partitioning={"type": "DAY", "field": "date_kr"},
            on_failure_callback=task_fail_slack_alert,
        )

        ride_funnel_task >> combine_task

    add_description_task = PythonOperator(
        task_id="description_task",
        python_callable=add_description.update_schema_description,
        op_kwargs={
            "table_fullname": "kr-co-vcnc-tada.tada_store.ride_funnel_metric",
            "schema_description": schema_description,
        },
        dag=dag,
    )

    combine_task >> add_description_task
