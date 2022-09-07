from datetime import datetime, timedelta
import os
from airflow import models
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
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
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 25),
    "email": ["kristoff@vcnc.co.kr"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "project_id": "kr-co-vcnc-tada",
}


schema_description = [
    {"name": "date_kr", "description": "파티션 date_kr"},
    {"name": "created_at", "description": "ride_id 생성 시점"},
    {"name": "user_id", "description": "유저 id"},
    {"name": "ride_id", "description": "호출뷰를 통해 생기거나 넛지 호출을 통해 생긴 라이드 id"},
    {
        "name": "original_ride_id",
        "description": "넛지를 통한 호출 시 새롭게 ride_id가 생기게 되는 데 넛지를 통한 호출 전 ride_id",
    },
    {
        "name": "call_view_type",
        "description": "호출뷰 클릭 차량 타입: PREMIUM, LITE, NXT, NEAR_TAXI, DAERI, LITE_TAXI_NUDGE, PREMIUM_NUDGE, NXT_NUDGE, NEAR_TAXI_NUDGE",
    },
    {
        "name": "suggestion_type",
        "description": "넛지 발생 상황: MATCH_DELAY(매칭 지연시), MATCH_FAIL(매칭 실패시)",
    },
    {"name": "is_continue", "description": "True: 넛지를 통한 호출, False: 호출뷰를 통한 호출"},
    {
        "name": "determined_type",
        "description": "최종 결정된 차량 타입:PREMIUM, LITE, NXT, DAERI, UNKNOWN(가까운 타다 호출시 주변차량이 없어서 호출 실패한 경우), 특이 사항: 1월 26일 이전 데이터는 로그가 잘못 적재되어 NULL로 채워져있음",
    },
]

date_suffix = ["{{ macros.ds_add(ds, +1) }}", "{{ macros.ds_add(ds, -0) }}"]
date_suffix_formatted = [
    "{{ macros.ds_format(macros.ds_add(ds, +1), '%Y-%m-%d', '%Y%m%d') }}",
    "{{ macros.ds_format(macros.ds_add(ds, -0), '%Y-%m-%d', '%Y%m%d') }}",
]
rule = ["today", "before_one_day"]

with models.DAG(
    dag_id="Extract-ride-category",
    description="ride-category",
    schedule_interval="10 * * * *",
    catchup=False,
    default_args=default_args,
) as dag:

    combine_task = DummyOperator(
        task_id="combine_tasks",
        trigger_rule="all_success",
        dag=dag,
    )

    for i in range(0, 2):
        ride_category_task = BigQueryOperator(
            dag=dag,
            bigquery_conn_id="google_cloud_for_tada",
            task_id=f"extract_ride_category_{rule[i]}",
            sql=utils.read_sql(
                os.path.join(os.environ["DAGS_FOLDER"], "sql/ride_category.sql")
            ).format(target_date=date_suffix[i]),
            use_legacy_sql=False,
            destination_dataset_table=f"kr-co-vcnc-tada.tada_ext.ride_category${date_suffix_formatted[i]}",
            write_disposition="WRITE_TRUNCATE",
            time_partitioning={"type": "DAY", "field": "date_kr"},
            on_failure_callback=task_fail_slack_alert,
        )
        ride_category_task >> combine_task

    add_description_task = PythonOperator(
        task_id="description_task",
        python_callable=add_description.update_schema_description,
        op_kwargs={
            "table_fullname": "kr-co-vcnc-tada.tada_ext.ride_category",
            "schema_description": schema_description,
        },
        dag=dag,
    )

    combine_task >> add_description_task
