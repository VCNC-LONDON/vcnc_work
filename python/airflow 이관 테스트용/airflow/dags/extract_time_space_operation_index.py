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
    "start_date": datetime(2021, 11, 25),
    "email": ["kristoff@vcnc.co.kr"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "project_id": "kr-co-vcnc-tada",
}

schema_description = [
    {"name": "time_interval_kr", "description": "30분 단위 시간"},
    {"name": "date_kr", "description": "파티션 date_kr"},
    {"name": "h3_l7_index", "description": "H3 L7 Index"},
    {"name": "h3_l7_name", "description": "H3 L7 Name"},
    {"name": "X", "description": "넥스트 순수 유효 세션"},
    {"name": "L", "description": "라이트 순수 유효 세션"},
    {"name": "P", "description": "플러스 순수 유효 세션"},
    {"name": "N", "description": "가까운타다 순수 유효 세션"},
    {"name": "M", "description": "혼합 유효 세션"},
    {"name": "T", "description": "통합 유효 세션"},
    {"name": "XX", "description": "넥스트 순수 유효 세션 - 넥스트 하차완료"},
    {"name": "LL", "description": "라이트 순수 유효 세션 - 라이트 하차완료"},
    {"name": "PP", "description": "플러스 순수 유효 세션 - 플러스 하차완료"},
    {"name": "NX", "description": "가까운타다 순수 유효 세션 - 넥스트 하차완료"},
    {"name": "NL", "description": "가까운타다 순수 유효 세션 - 라이트 하차완료"},
    {"name": "NP", "description": "가까운타다 순수 유효 세션 - 플러스 하차완료"},
    {"name": "MX", "description": "혼합 유효 세션 - 넥스트 하차완료"},
    {"name": "ML", "description": "혼합 유효 세션 - 라이트 하차완료"},
    {"name": "MP", "description": "혼합 유효 세션 - 플러스 하차완료"},
    {"name": "TT", "description": "통합 전체 하차완료"},
    {"name": "TX", "description": "넥스트 전체 하차완료"},
    {"name": "TL", "description": "라이트 전체 하차완료"},
    {"name": "TP", "description": "플러스 전체 하차완료"},
    {"name": "X_surge", "description": "넥스트 하차완료 가중평균 서지"},
    {"name": "L_surge", "description": "라이트 하차완료 가중평균 서지"},
    {"name": "P_surge", "description": "플러스 하차완료 가중평균 서지"},
    {"name": "X_drr", "description": "넥스트 순수 하차완료율"},
    {"name": "L_drr", "description": "라이트 순수 하차완료율"},
    {"name": "P_drr", "description": "플러스 순수 하차완료율"},
    {"name": "N_drr", "description": "가까운타다 순수 하차완료율"},
    {"name": "M_drr", "description": "혼합 하차완료율"},
    {"name": "T_drr", "description": "통합 전체 하차완료율"},
    {"name": "X_op_demand", "description": "넥스트 운영 수요"},
    {"name": "L_op_demand", "description": "라이트 운영 수요"},
    {"name": "P_op_demand", "description": "플러스 운영 수요"},
    {"name": "X_op_drr", "description": "넥스트 운영 하차완료율"},
    {"name": "L_op_drr", "description": "라이트 운영 하차완료율"},
    {"name": "P_op_drr", "description": "플러스 운영 하차완료율"},
    {"name": "X_supply", "description": "넥스트 유효 공급"},
    {"name": "L_supply", "description": "라이트 유효 공급"},
    {"name": "P_supply", "description": "플러스 유효 공급"},
    {"name": "h3_l7_lat", "description": "H3 L7 lat"},
    {"name": "h3_l7_lng", "description": "H3 L7 lng"},
]

date_suffix = ["{{ macros.ds_add(ds, +1) }}", "{{ macros.ds_add(ds, -0) }}"]
date_suffix_formatted = [
    "{{ macros.ds_format(macros.ds_add(ds, +1), '%Y-%m-%d', '%Y%m%d') }}",
    "{{ macros.ds_format(macros.ds_add(ds, -0), '%Y-%m-%d', '%Y%m%d') }}",
]

rule = ["today", "before_one_day"]

with models.DAG(
    dag_id="Extract-time_space_operation_index",
    description="time_space_operation_index",
    schedule_interval="30 * * * *",
    catchup=False,
    default_args=default_args,
) as dag:
    combine_task = DummyOperator(
        task_id="combine_tasks",
        trigger_rule="all_success",
        dag=dag,
    )

    for i in range(0, 2):
        extract_time_space_operation_task = BigQueryOperator(
            dag=dag,
            bigquery_conn_id="google_cloud_for_tada",
            task_id=f"extract_time_space_operation_index_{rule[i]}",
            sql=utils.read_sql(
                os.path.join(
                    os.environ["DAGS_FOLDER"], "sql/time_space_operation_index.sql"
                )
            ).format(target_date=date_suffix[i]),
            use_legacy_sql=False,
            destination_dataset_table=f"kr-co-vcnc-tada.tada_store.time_space_operation_index${date_suffix_formatted[i]}",
            write_disposition="WRITE_TRUNCATE",
            time_partitioning={"type": "DAY", "field": "date_kr"},
            on_failure_callback=task_fail_slack_alert,
        )

        extract_time_space_operation_task >> combine_task

    add_description_task = PythonOperator(
        task_id="description_task",
        python_callable=add_description.update_schema_description,
        op_kwargs={
            "table_fullname": "kr-co-vcnc-tada.tada_store.time_space_operation_index",
            "schema_description": schema_description,
        },
        dag=dag,
    )

    combine_task >> add_description_task
