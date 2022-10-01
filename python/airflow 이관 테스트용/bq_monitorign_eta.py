from datetime import datetime, timedelta
import requests
import json
import pandas as pd
from airflow import models
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from dependencies import slack_operator, add_description

SLACK_CONN_ID = "slack"

class JinjaPythonOperator(PythonOperator):
    template_fields = ("templates_dict", "op_args")


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
    "start_date": datetime(2022, 9, 14),
    "email": ["london@vcnc.co.kr"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_exponential_backoff": True,
    "retry_delay": timedelta(minutes=5),
    "max_retry_delay": timedelta(minutes=20),
    "project_id": "kr-co-vcnc-tada",
}

dash_date = ["{{ macros.ds_add(ds, -0) }}", "{{ macros.ds_add(ds, -1) }}"]

nodash_date = [
    "{{ macros.ds_format(macros.ds_add(ds, -0), '%Y-%m-%d', '%Y%m%d') }}",
    "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d') }}",
]

intvl_start = "{{ data_interval_start }}"
intvl_end = "{{ data_interval_end }}"

extract_bigquery_usage_raw_query = """
SELECT
  resource.labels.project_id,
  severity, 
  protopayload_auditlog.status.code AS status_code, 
  protopayload_auditlog.status.message AS status_message,
  protopayload_auditlog.methodName AS method_name,
  DATETIME(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime, "Asia/Seoul") AS start_datetime,
  DATETIME(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime, "Asia/Seoul") AS end_datetime,
  protopayload_auditlog.authenticationInfo.principalEmail AS user,
  protopayload_auditlog.requestMetadata.callerSuppliedUserAgent	AS user_agent,
  protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes/1024/1024/1000 AS total_billed_gb,
  protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query AS query,
  DATE(DATETIME(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime, "Asia/Seoul")) AS date_kr
FROM
  `kr-co-vcnc-tada.bigquery_log.cloudaudit_googleapis_com_data_access_*`
WHERE
  _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB("{logical_date}", INTERVAL 1 day)) AND FORMAT_DATE("%Y%m%d", "{logical_date}")
  AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime BETWEEN TIMESTAMP_ADD("{data_interval_start}", INTERVAL -15 MINUTE) AND TIMESTAMP_ADD("{data_interval_end}", INTERVAL 15 MINUTE)
"""

bigquery_usage_raw_schema_description = [
    {"name": "project_id", "description": "project_id : kr-co-vcnc-tada"},
    {"name": "severity", "description": "severity(심각도) Level : INFO, ERROR"},
    {"name": "status_code", "description": "Audit log의 status code"},
    {"name": "status_message", "description": "Audit log의 status message(오류 메세지)"},
    {"name": "method_name", "description": "사용한 method : jobservice.jobcompleted"},
    {"name": "start_datetime", "description": "쿼리를 실행한 시간(한국 시간, datetime)"},
    {"name": "end_datetime", "description": "쿼리가 종료된 시간(한국 시간, datetime)"},
    {"name": "user", "description": "user 이메일. 서비스 계정도 포함"},
    {"name": "user_agent", "description": "user agent 정보"},
    {"name": "total_billed_gb", "description": "빅쿼리 조회할 때 과금된 용량(GB)"},
    {"name": "query", "description": "사용한 쿼리"},
    {"name": "date_kr", "description": "파티션 일자(한국 기준)"},
]

aggregate_bigquery_usage_daily_query = """
SELECT
  DATE(start_datetime) AS start_date,
  user,
  ROUND(SUM(total_billed_gb)/1024, 3) AS sum_total_billed_terabytes
FROM `kr-co-vcnc-tada.bigquery_log.bigquery_usage_raw`
WHERE
  date_kr = "{execute_date}"
  AND severity = "INFO"
GROUP BY
  start_date,
  user
HAVING sum_total_billed_terabytes > 0
"""


def send_bigquery_usage(execute_date, **kwargs):
    bigquery_monitoring_threashold_quota = 1
    slack_webhook_url = models.Variable.get("bigquery_monitoring_webhook_url")

    slack_channel_id = "#data_bigquery_monitoring"

    monitoring_query = f"""
    SELECT
      start_date,
      user,
      sum_total_billed_terabytes
    FROM `kr-co-vcnc-tada.bigquery_log.bigquery_usage_daily`
    WHERE
      start_date = '{execute_date}'
      AND sum_total_billed_terabytes >= {bigquery_monitoring_threashold_quota}
    """

    df = pd.read_gbq(
        query=monitoring_query, dialect="standard", project_id="kr-co-vcnc-tada"
    )
    bigquery_usage_dict = df.to_dict()
    bigquery_usage_message = []

    for user in bigquery_usage_dict["user"]:
        bigquery_usage_message.append(
            "계정 : "
            + bigquery_usage_dict["user"][user]
            + "\n"
            + "일일 사용량 : "
            + str(bigquery_usage_dict["sum_total_billed_terabytes"][user])
            + "\n\n"
        )

    message_result = "".join(bigquery_usage_message)
    send_message_result = (
        f"{execute_date}에 {bigquery_monitoring_threashold_quota}TB이상 사용한 사용자입니다. \n"
        "쿼리를 확인하여 튜닝 또는 조치가 필요합니다. \n\n"
        + message_result
        + "자세한 내용을 확인하고 싶으실 경우 아래의 링크를 클릭 해주세요.\n"
        "url : https://secure.holistics.io/queries/292608"
    )
    slack_message = (
        ":vcnc::warning:"
        + " *VCNC 빅쿼리 사용량 모니터링* \n"
        + "```"
        + send_message_result
        + "```"
    )

    payload = {
        "channel": slack_channel_id,
        "text": slack_message,
        "username": "VCNC_빅쿼리_사용량_모니터링봇",
    }
    requests.post(
        slack_webhook_url,
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"},
    )


with models.DAG(
    dag_id="Extract-bigquery_usage",
    description="extract_bigquery_usage",
    schedule_interval="0 */1 * * *",
    default_args=default_args,
    catchup=False,
) as dag:
    combine_task = DummyOperator(
        task_id="combine_tasks",
        trigger_rule="all_success",
        dag=dag,
    )

    for i in range(0, 2):
        rule = ["before_one_day", "before_two_day"]

        extract_bigquery_usage_raw_task = BigQueryOperator(
            dag=dag,
            bigquery_conn_id="google_cloud_for_tada",
            task_id=f"extract_bigquery_usage_{rule[i]}",
            sql=extract_bigquery_usage_raw_query.format(execute_date=dash_date[i]),
            use_legacy_sql=False,
            destination_dataset_table=f"kr-co-vcnc-tada.bigquery_log.bigquery_usage_raw${nodash_date[i]}",
            write_disposition="WRITE_TRUNCATE",
            time_partitioning={"type": "DAY", "field": "date_kr"},
            on_failure_callback=task_fail_slack_alert,
        )

        aggregate_bigquery_usage_daily_task = BigQueryOperator(
            dag=dag,
            bigquery_conn_id="google_cloud_for_tada",
            task_id=f"aggregate_bigquery_usage_daily_{rule[i]}",
            sql=aggregate_bigquery_usage_daily_query.format(execute_date=dash_date[i]),
            use_legacy_sql=False,
            destination_dataset_table=f"kr-co-vcnc-tada.bigquery_log.bigquery_usage_daily${nodash_date[i]}",
            write_disposition="WRITE_TRUNCATE",
            time_partitioning={"type": "DAY", "field": "start_date"},
            on_failure_callback=task_fail_slack_alert,
        )

        (
            extract_bigquery_usage_raw_task
            >> aggregate_bigquery_usage_daily_task
            >> combine_task
        )

    add_bigquery_usage_raw_description_task = PythonOperator(
        dag=dag,
        task_id="description_task",
        python_callable=add_description.update_schema_description,
        op_kwargs={
            "table_fullname": "kr-co-vcnc-tada.bigquery_log.bigquery_usage_raw",
            "schema_description": bigquery_usage_raw_schema_description,
        },
        on_failure_callback=task_fail_slack_alert,
    )

    send_bigquery_usage_task = JinjaPythonOperator(
        dag=dag,
        task_id="send_bigquery_usage",
        python_callable=send_bigquery_usage,
        provide_context=True,
        op_args=[dash_date[0]],
        on_failure_callback=task_fail_slack_alert,
    )

    combine_task >> add_bigquery_usage_raw_description_task >> send_bigquery_usage_task
