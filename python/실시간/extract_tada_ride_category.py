from datetime import datetime, timedelta
from airflow import models
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from dependencies import slack_operator, add_description


SLACK_CONN_ID = 'slack'

def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed.
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )
    failed_alert = slack_operator.SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='tada-airflow',
        dag=dag)
    return failed_alert.execute(context=context)

default_args = {
    'owner': 'kristoff',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 25),
    'email': ['kristoff@vcnc.co.kr'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'project_id': 'kr-co-vcnc-tada'
}

# dag 객체 생성
extract_ride_category_query = """
WITH SERVER_LOG AS (
  SELECT
    server_log.date_kr,
    server_log.timeMs AS time_ms,
    server_log.type AS log_type,
    server_log.uuid,
    TIMESTAMP_TRUNC(TIMESTAMP_ADD(TIMESTAMP_MILLIS(server_log.ride.createdAtMs), INTERVAL 500 MILLISECOND), SECOND) AS created_at,
    server_log.caller.userId AS user_id,
    server_log.alternativeRideTypes AS alternative_ride_types,
    server_log.ride.suggestionType AS suggestion_type,
    server_log.ride.id AS ride_id,
    server_log.ride.originalRideId AS original_ride_id,
    server_log.ride.rideType AS ride_type, -- 가까운 타다 여부 (NEAR_TAXI, LITE, PLUS, NEXT, DAERI)
    -- 호출가능한 가장 가까운 주변차의 타입으로 결정됐을 때(NEAREST_NEARBY_VEHICLE_TYPE), 호출가능한 주변차가 없는 경우(UNKNOWN) (현재 로직상 이경우 determinedRideType이 PREMIUM)
    -- 가까운 타다의 경우 ride_id별 시간순으로 봤을때 determinedRideType이 계속 변화하는 속성이 있고, 제일 마지막 value값이 최종 determinedRideType이다.
    LAST_VALUE(ride.nearRideLog.cause) OVER (PARTITION BY server_log.ride.id ORDER BY server_log.timeMs ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as near_taxi_cause,
    LAST_VALUE(ride.nearRideLog.determinedRideType) OVER (PARTITION BY server_log.ride.id ORDER BY server_log.timeMs ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as near_taxi_determined_ride_type,
  FROM
    tada.server_log_parquet AS server_log
  WHERE
    server_log.date_kr = "{target_date}"
    AND server_log.type in ('USER_REQUEST_RIDE', 'USER_REQUEST_ALTERNATIVE_RIDE', 'DISPATCHER_DETERMINE_NEAR_RIDE_TYPE')
  )

, RIDE_CATEGORY AS(
  SELECT
    date_kr,
    created_at,
    user_id,
    ride_id,
    original_ride_id,
    near_taxi_cause,
    near_taxi_determined_ride_type,
    -- 호출뷰에서의 콜
    IF(log_type = 'USER_REQUEST_RIDE' and ride_type = 'LITE', True, False) AS is_lite_call,
    IF(log_type = 'USER_REQUEST_RIDE' and ride_type = 'PREMIUM', True, False) AS is_plus_call,
    IF(log_type = 'USER_REQUEST_RIDE' and ride_type = 'NXT', True, False) AS is_next_call,
    IF(log_type = 'USER_REQUEST_RIDE' and ride_type = 'NEAR_TAXI', True, False) AS is_near_taxi_call,
    IF(log_type = 'USER_REQUEST_RIDE' and ride_type = 'DAERI', True, False) AS is_daeri_call,
    -- 매칭 지연후 콜
    IF(log_type = 'USER_REQUEST_ALTERNATIVE_RIDE' and ride_type ='LITE' and suggestion_type = 'ON_DISPATCH_FAILED', True, False) AS is_lite_call_after_delay_match,
    IF(log_type = 'USER_REQUEST_ALTERNATIVE_RIDE' and ride_type ='PREMIUM' and suggestion_type = 'ON_DISPATCH_FAILED', True, False) AS is_plus_call_after_delay_match,
    IF(log_type = 'USER_REQUEST_ALTERNATIVE_RIDE' and ride_type ='NXT' and suggestion_type = 'ON_DISPATCH_FAILED', True, False) AS is_next_call_after_delay_match,
    IF(log_type = 'USER_REQUEST_ALTERNATIVE_RIDE' and ride_type ='NEAR_TAXI' and suggestion_type = 'ON_DISPATCH_FAILED', True, False) AS is_near_taxi_call_after_delay_match,
    -- 매칭 실패후 콜
    IF(log_type = 'USER_REQUEST_ALTERNATIVE_RIDE' and ride_type ='LITE' and suggestion_type = 'ON_RIDE_TIMEOUT', True, False) AS is_lite_call_after_fail_match,
    IF(log_type = 'USER_REQUEST_ALTERNATIVE_RIDE' and ride_type ='PREMIUM' and suggestion_type = 'ON_RIDE_TIMEOUT', True, False) AS is_plus_call_after_fail_match,
    IF(log_type = 'USER_REQUEST_ALTERNATIVE_RIDE' and ride_type ='NXT' and suggestion_type = 'ON_RIDE_TIMEOUT', True, False) AS is_next_call_after_fail_match,
    IF(log_type = 'USER_REQUEST_ALTERNATIVE_RIDE' and ride_type ='NEAR_TAXI' and suggestion_type = 'ON_RIDE_TIMEOUT', True, False) AS is_near_taxi_call_after_fail_match,
  FROM
    SERVER_LOG
  WHERE
    log_type in ('USER_REQUEST_RIDE', 'USER_REQUEST_ALTERNATIVE_RIDE')
  )

, RIDE_CATEGORY_EXT AS (
  SELECT
    date_kr,
    created_at,
    user_id,
    ride_id,
    original_ride_id,
    -- 호출뷰를 통한 호출
    CASE WHEN is_plus_call = True THEN 'PREMIUM'
         WHEN is_lite_call = True THEN 'LITE'
         WHEN is_next_call = True THEN 'NXT'
         WHEN is_near_taxi_call = True THEN 'NEAR_TAXI'
         WHEN is_daeri_call = True THEN 'DAERI'
         WHEN is_near_taxi_call_after_delay_match = True or is_near_taxi_call_after_fail_match = True THEN 'NEAR_TAXI_NUDGE'
         WHEN is_lite_call_after_delay_match = True or is_lite_call_after_fail_match = True THEN 'LITE_NUDGE'
         WHEN is_plus_call_after_delay_match = True or is_plus_call_after_fail_match = True THEN 'PREMIUM_NUDGE'
         WHEN is_next_call_after_delay_match = True or is_next_call_after_fail_match = True THEN 'NXT_NUDGE'
         ELSE NULL END AS call_view_type,
    -- 넛지 타입 (매칭 지연시 넛지: MATCH_DELAY, 매칭 실패시 넛지: MATCH_FAIL)
    CASE WHEN is_near_taxi_call_after_delay_match = True OR is_lite_call_after_delay_match = True OR is_plus_call_after_delay_match = True OR is_next_call_after_delay_match = True THEN 'MATCH_DELAY'
         WHEN is_near_taxi_call_after_fail_match = True OR is_lite_call_after_fail_match = True OR is_plus_call_after_fail_match = True OR is_next_call_after_fail_match = True THEN 'MATCH_FAIL'
         ELSE NULL END AS suggestion_type,
    -- 넛지를 통한 호출 여부 (True, False)
    IF(is_near_taxi_call_after_delay_match = True OR is_lite_call_after_fail_match = True OR is_plus_call_after_delay_match = True OR is_next_call_after_fail_match = True, True, False) AS is_continue,
    -- 최종 타입
    CASE WHEN is_lite_call = True THEN 'LITE'
         WHEN is_lite_call_after_delay_match = True THEN 'LITE'
         WHEN is_lite_call_after_fail_match = True THEN 'LITE'
         WHEN is_plus_call = True THEN 'PREMIUM'
         WHEN is_plus_call_after_delay_match = True THEN 'PREMIUM'
         WHEN is_plus_call_after_fail_match = True THEN 'PREMIUM'
         WHEN is_next_call = True THEN 'NXT'
         WHEN is_next_call_after_delay_match = True THEN 'NXT'
         WHEN is_next_call_after_fail_match = True THEN 'NXT'
         WHEN is_daeri_call = True THEN 'DAERI'
         WHEN (is_near_taxi_call = True AND near_taxi_cause LIKE 'NEAREST_NEARBY_VEHICLE_TYPE') THEN near_taxi_determined_ride_type
         WHEN (is_near_taxi_call = True AND near_taxi_cause = 'UNKNOWN') THEN 'UNKNOWN' -- 매칭 실패시 어떤 타입의 차량도 존재하지 않음
         WHEN (is_near_taxi_call_after_delay_match = True AND near_taxi_cause LIKE 'NEAREST_NEARBY_VEHICLE_TYPE') THEN near_taxi_determined_ride_type
         WHEN (is_near_taxi_call_after_delay_match = True AND near_taxi_cause = 'UNKNOWN') THEN 'UNKNOWN' -- 매칭 실패시 어떤 타입의 차량도 존재하지 않음
         WHEN (is_near_taxi_call_after_fail_match = True AND near_taxi_cause LIKE 'NEAREST_NEARBY_VEHICLE_TYPE') THEN near_taxi_determined_ride_type
         WHEN (is_near_taxi_call_after_fail_match = True AND near_taxi_cause = 'UNKNOWN') THEN 'UNKNOWN' -- 매칭 실패시 어떤 타입의 차량도 존재하지 않음
         END AS determined_type
  FROM
    RIDE_CATEGORY
  )

SELECT
  *
FROM
  RIDE_CATEGORY_EXT
"""

schema_description = [
    {'name': 'date_kr', 'description': '파티션 date_kr'},
    {'name': 'created_at', 'description': 'ride_id 생성 시점'},
    {'name': 'user_id', 'description': '유저 id'},
    {'name': 'ride_id', 'description': '호출뷰를 통해 생기거나 넛지 호출을 통해 생긴 라이드 id'},
    {'name': 'original_ride_id', 'description': '넛지를 통한 호출 시 새롭게 ride_id가 생기게 되는 데 넛지를 통한 호출 전 ride_id'},
    {'name': 'call_view_type', 'description': '호출뷰 클릭 차량 타입: PREMIUM, LITE, NXT, NEAR_TAXI, DAERI, LITE_TAXI_NUDGE, PREMIUM_NUDGE, NXT_NUDGE, NEAR_TAXI_NUDGE'},
    {'name': 'suggestion_type', 'description': '넛지 발생 상황: MATCH_DELAY(매칭 지연시), MATCH_FAIL(매칭 실패시)'},
    {'name': 'is_continue', 'description': 'True: 넛지를 통한 호출, False: 호출뷰를 통한 호출'},
    {'name': 'determined_type', 'description': '최종 결정된 차량 타입:PREMIUM, LITE, NXT, DAERI, UNKNOWN(가까운 타다 호출시 주변차량이 없어서 호출 실패한 경우), 특이 사항: 1월 26일 이전 데이터는 로그가 잘못 적재되어 NULL로 채워져있음'}
]

date_suffix = ["{{ macros.ds_add(ds, +1) }}", "{{ macros.ds_add(ds, -0) }}"]
date_suffix_formatted = [
    "{{ macros.ds_format(macros.ds_add(ds, +1), '%Y-%m-%d', '%Y%m%d') }}",
    "{{ macros.ds_format(macros.ds_add(ds, -0), '%Y-%m-%d', '%Y%m%d') }}",
]
rule = ["today", "before_one_day"]

with models.DAG(
        dag_id='Extract-ride-category',
        description='ride-category',
        schedule_interval='10 * * * *',
        catchup=False,
        default_args=default_args) as dag:

    combine_task = DummyOperator(
        task_id="combine_tasks",
        trigger_rule="all_success",
        dag=dag,
    )

    for i in range(0, 2):
        ride_category_task = BigQueryOperator(
            dag=dag,
            bigquery_conn_id='google_cloud_for_tada',
            task_id=f"extract_ride_category_{rule[i]}",
            sql=extract_ride_category_query.format(target_date=date_suffix[i]),
            use_legacy_sql=False,
            destination_dataset_table=f'kr-co-vcnc-tada.tada_ext.ride_category${date_suffix_formatted[i]}',
            write_disposition='WRITE_TRUNCATE',
            time_partitioning={'type': 'DAY', 'field': 'date_kr'},
            on_failure_callback=task_fail_slack_alert
        )
        ride_category_task >> combine_task

    add_description_task = PythonOperator(
        task_id='description_task',
        python_callable=add_description.update_schema_description,
        op_kwargs={'table_fullname': 'kr-co-vcnc-tada.tada_ext.ride_category',
                   'schema_description': schema_description},
        dag=dag
    )

    combine_task >> add_description_task
