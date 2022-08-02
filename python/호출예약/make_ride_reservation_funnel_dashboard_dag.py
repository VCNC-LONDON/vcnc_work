from datetime import datetime, timedelta
from airflow import models, macros
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from dependencies import slack_operator

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
    "start_date": datetime(2022, 1, 18),
    "depends_on_past": False,
    "email": ["london@vcnc.co.kr"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "project_id": "kr-co-vcnc-tada",
}

query = """
WITH 

firebase AS (
    SELECT 
        date_kr,
        user_pseudo_id,
        event_datetime,
        event_name,
        MAX( IF(e.key = "view_name", e.string_value, NULL) ) AS view_name,
        MAX( IF(e.key = "page_location",e.string_value, NULL) ) AS page_location,
        MAX( IF(e.key = "component_name",e.string_value, NULL) ) AS component_name,
        MAX( IF(e.key = "ride_estimation_uuid",e.string_value, NULL) ) AS ride_estimation_uuid,
        MAX( IF(e.key = "product_id",e.string_value, NULL) ) AS product_id,
        MAX( IF(e.key = "error_code",e.string_value, NULL) ) AS error_code,
        MAX( IF(e.key = "reserve_timestamp",e.string_value, NULL) ) AS reserve_timestamp,
    FROM 
        `kr-co-vcnc-tada.tada_ext.firebase_app_event`, UNNEST(event_params) AS e
    WHERE
        date_kr = "{target_date}"
        AND event_name IN ("web_page_view", "web_click", "view_click")
        AND app_id = "kr.co.vcnc.tada"
    GROUP BY 
        1, 2, 3, 4
)

select 
    date_kr,
    COUNT(DISTINCT IF(view_name IN ("HOME_FAVORITE_ITEM", "LOCATION_MAP_PICKER_CONFIRM"), user_pseudo_id, null)) AS estimate_view_come_user_cnt,
    COUNT(DISTINCT IF(REGEXP_CONTAINS(page_location,"/booking") AND event_name ="web_page_view", user_pseudo_id, NULL)) AS reserve_page_come_user_cnt,
    COUNT(DISTINCT IF( (REGEXP_CONTAINS(page_location,"/booking") AND event_name ="web_page_view") OR (REGEXP_CONTAINS(page_location,"/booking") AND event_name ="web_click" AND component_name = "SELECTOR_RIDE_TYPE"), user_pseudo_id, NULL)) AS reserve_page_type_select_user_cnt,
    COUNT(DISTINCT IF( (REGEXP_CONTAINS(page_location,"/booking") AND event_name ="web_click" AND reserve_timestamp IS NOT NULL), user_pseudo_id, NULL)) AS reserve_confirm_view_user_cnt,
    COUNT(DISTINCT IF( (REGEXP_CONTAINS(page_location,"/booking") AND event_name ="web_click" AND reserve_timestamp IS NOT NULL AND component_name = "submit"), user_pseudo_id, NULL)) AS reserve_confirm_view_confirm_user_cnt,
    COUNT(DISTINCT IF( (REGEXP_CONTAINS(page_location,"/booking") AND event_name ="web_click" AND component_name = "submit" AND error_code = "RIDE_RESERVATION_EXCEED_LIMIT"), user_pseudo_id, NULL)) AS reserve_error_exceed_limit,
    COUNT(DISTINCT IF( (REGEXP_CONTAINS(page_location,"/booking") AND event_name ="web_click" AND component_name = "submit" AND error_code = "RIDE_RESERVATION_EXPECTED_PICK_UP_AT_NOT_IN_AVAILABLE_TIME"), user_pseudo_id, NULL)) AS reserve_error_time_not_availd,
    COUNT(DISTINCT IF( (REGEXP_CONTAINS(page_location,"/booking") AND event_name ="web_click" AND component_name = "submit" AND error_code = "RIDE_RESERVATION_ALREADY_EXISTS_IN_EXPECTED_PICK_UP_AT_LIST"), user_pseudo_id, NULL)) AS reserve_error_time_already_select,
    COUNT(DISTINCT IF( (REGEXP_CONTAINS(page_location,"/booking") AND event_name ="web_click" AND component_name = "submit" AND error_code = "RIDE_RESERVATION_ALREADY_RESERVED"), user_pseudo_id, NULL)) AS reserve_error_already_reserve
from firebase 
group by 1
"""

date_suffix = [
    "{{ macros.ds_format(macros.ds_add(ds, -0), '%Y-%m-%d', '%Y%m%d') }}",
    "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d') }}",
]

with models.DAG(
    dag_id="Make_ride_reservation_funnel_dashboard",
    description="extract ride reservation funnel dashboard data from firebase data",
    schedule_interval="0 3 * * *",
    catchup=False,
    default_args=default_args,
) as dag:

    combine_task = DummyOperator(
        task_id="combine_tasks",
        trigger_rule="all_success",
        dag=dag,
    )

    for i in range(0, 2):
        rule = ["before_one_day", "before_two_day"]

        extract_ride_reservation_funnel_data = BigQueryOperator(
            dag=dag,
            bigquery_conn_id="google_cloud_for_tada",
            task_id=f"extract_ride_reservation_funnel_data_{rule[i]}",
            sql=query.format(target_date=date_suffix[i]),
            use_legacy_sql=False,
            destination_dataset_table=f"kr-co-vcnc-tada.tada_reservation.ride_reservation_funnel_data${date_suffix[i]}",
            write_disposition="WRITE_TRUNCATE",
            time_partitioning={"type": "DAY", "field": "date_kr"},
            on_failure_callback=task_fail_slack_alert,
        )

        extract_ride_reservation_funnel_data >> combine_task