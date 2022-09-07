from datetime import datetime
from dependencies import slack_operator, add_description, utils
import os
from airflow import models
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
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
    "start_date": datetime(2022, 9, 6),
    "email": ["london@vcnc.co.kr"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_exponential_backoff": True,
    "project_id": "kr-co-vcnc-tada",
}

schema_description = [
    {"name": "id", "description": "라이드 ID"},
    {"name": "type", "description": "라이드 타입. BASIC / ASSIST / PREMIUM 중 하나."},
    {"name": "created_at", "description": "라이드 요청 시각."},
    {"name": "status", "description": "라이드 상태. PENDING (배차 전) -> ACCEPTED (배차 후) -> ARRIVED (드라이버 출발지 도착) -> PICKED_UP (유저 탑승) -> DROPPED_OFF (하차 완료) 로 진행되고, 중간에 CANCELED 로 빠질 수 있음."},
    {"name": "rider_id", "description": "유저 ID = tada.user 테이블의 id."},
    {"name": "payment_method_id", "description": "결제 수단 ID = tada.payment_method 테이블의 id."},
    {"name": "coupon_id", "description": "쿠폰 ID = tada.ride_coupon 테이블의 id."},
    {"name": "origin_eta_multiplier", "description": "출발지 ETA 보정 배율. 서드 파티 API 호출 결과에 이 값을 곱해서 보정하여 유저에게 노출 시킵니다."},
    {"name": "driver_id", "description": "드라이버 ID = tada.driver 테이블의 id."},
    {"name": "vehicle_id", "description": "차량 ID = tada.vehicle 테이블의 id."},
    {"name": "surge_percentage", "description": "서지 배율. 100 이상의 정수 값."},
    {"name": "origin_lng", "description": "출발지 경도."},
    {"name": "origin_lat", "description": "출발지 위도."},
    {"name": "origin_name", "description": "출발지 주소 (세부)."},
    {"name": "origin_address", "description": "출발지 주소 (전체)."},
    {"name": "pickup_lng", "description": "유저 탑승 경도."},
    {"name": "pickup_lat", "description": "유저 탑승 위도."},
    {"name": "pickup_name", "description": "유저 탑승 주소 (세부)."},
    {"name": "pickup_address", "description": "유저 탑승 주소 (전체)."},
    {"name": "picked_up_at", "description": "유저 탑승 시각."},
    {"name": "destination_lng", "description": "도착지 경도."},
    {"name": "destination_lat", "description": "도착지 위도."},
    {"name": "destination_name", "description": "도착지 주소 (세부)."},
    {"name": "destination_address", "description": "도착지 주소 (전체)."},
    {"name": "dropoff_lng", "description": "유저 하차 경도."},
    {"name": "dropoff_lat", "description": "유저 하차 위도."},
    {"name": "dropoff_name", "description": "유저 하차 주소 (세부)."},
    {"name": "dropoff_address", "description": "유저 하차 주소 (전체)."},
    {"name": "dropped_off_at", "description": "유저 하차 시각."},
    {"name": "accepted_at", "description": "드라이버 배차 수락 시각."},
    {"name": "arrived_at", "description": "드라이버 출발지 도착 버튼 클릭 시각."},
    {"name": "cancellation_cause", "description": "취소 원인. DISPATCH_TIMEOUT (배차 실패) / DRIVER_CANCELLED / RIDER_CANCELLED 중 하나."},
    {"name": "cancellation_reason", "description": "취소 사유."},
    {"name": "cancelled_at", "description": "취소 시각."},
    {"name": "distance_meters", "description": "유저 탑승 ~ 유저 하차 이동 거리. 단위 = 미터."},
    {"name": "estimation_distance_meters", "description": "(예상) 유저 탑승 ~ 유저 하차 이동 거리. 단위 = 미터. 서드 파티 API 호출 결과."},
    {"name": "estimation_duration_seconds", "description": "(예상) 유저 탑승 ~ 유저 하차 이동 시간. 단위 = 초. 서드 파티 API 호출 결과."},
    {"name": "estimation_min_cost", "description": "예상 최소 요금. 단위 = 원. 쿠폰 반영 후."},
    {"name": "estimation_max_cost", "description": "예상 최대 요금. 단위 = 원. 쿠폰 반영 후."},
    {"name": "estimation_original_min_cost", "description": "예상 최소 요금. 단위 = 원. 쿠폰 반영 전."},
    {"name": "estimation_original_max_cost", "description": "예상 최대 요금. 단위 = 원. 쿠폰 반영 전."},
    {"name": "receipt_basic_fee", "description": "기본 요금. 단위 = 원."},
    {"name": "receipt_distance_based_fee", "description": "거리 요금. 단위 = 원."},
    {"name": "receipt_time_based_fee", "description": "시간 요금. 단위 = 원."},
    {"name": "receipt_rent_fee", "description": "렌트 요금. 단위 = 원. 실제 요금 계산은 [기본 요금 + 거리 요금 + 시간 요금] 으로 한 뒤 [렌트 요금 + 기사 요금] 으로 적절히 쪼개서 노출합니다."},
    {"name": "receipt_drive_fee", "description": "기사 요금. 단위 = 원. 실제 요금 계산은 [기본 요금 + 거리 요금 + 시간 요금] 으로 한 뒤 [렌트 요금 + 기사 요금] 으로 적절히 쪼개서 노출합니다."},
    {"name": "receipt_tollgate_fee", "description": "톨게이트 요금. 단위 = 원."},
    {"name": "receipt_cancellation_fee", "description": "취소 요금. 단위 = 원. 드라이버가 출발지 도착 한 뒤 몇분 초과 후 라이더가 취소하면 부과됩니다."},
    {"name": "receipt_discount_amount", "description": "할인. 단위 = 원. 첫 탑승 할인, 쿠폰 할인 등 직원 할인 외 할인."},
    {"name": "receipt_employee_discount_amount", "description": "직원 할인. 단위 = 원."},
    {"name": "receipt_refund_amount", "description": "환불. 단위 = 원."},
    {"name": "receipt_total", "description": "할인 후 매출. 단위 = 원."},
    {"name": "receipt_extra", "description": "e.g. 라이더가 카드 바꿔 결제해주세요! CS 요청 시 receipt_total = 0 이 되고, 이 값으로 옮겨감."},
    {"name": "assist_registration_id", "description": "type = 'ASSIST' 일때 어떤 user_assist_registration 자격 증명이 사용됬는 지."},
    {"name": "cancellation_reason_type", "description": "취소 사유 카테고리. 몇가지 값 중 하나."},
    {"name": "price_elasticity_testing_percentage", "description": "가격 탄력성 테스트 적용 값. 97 / 103 / null 중 하나."},
    {"name": "arrived_at_destination_at", "description": ""},
    {"name": "adjusted_distance_meters", "description": ""},
    {"name": "payment_profile_id", "description": ""},
    {"name": "biz_reason", "description": ""},
    {"name": "biz_reason_text", "description": ""},
    {"name": "rider_virtual_phone_number", "description": ""},
    {"name": "rider_virtual_phone_number_expiry", "description": ""},
    {"name": "is_valid", "description": "유효호출 여부(tada_store 의 last_5_min 과 같음)"},
    {"name": "origin_eta", "description": "출발지 이동 ETA 계산 기록"},
    {"name": "destination_eta", "description": "도착지 이동 ETA 계산 기록"},
    {"name": "schedule_start_at", "description": ""},
    {"name": "schedule_vehicle_zone_id", "description": ""},
    {"name": "schedule_working_type", "description": ""},
    {"name": "assigned_area_name", "description": ""},
    {"name": "assigned_area_start_at", "description": ""},
    {"name": "assigned_area_end_at", "description": ""},
    {"name": "initial_origin_eta", "description": "최초 계산된 ETA "},
    {"name": "cancellation_fee_type", "description": ""},
    {"name": "should_waive_cancellation_fee", "description": ""},
    {"name": "biz_discount_amount", "description": ""},
    {"name": "biz_discount_rate", "description": ""},
    {"name": "date_kr", "description": "한국 날짜 (파티션 용)"},
]

execute_date = "{{ds}}"
execute_date_formatted = "{{ds_nodash}}"
execute_ts = "{{ts}}"

with models.DAG(
    dag_id="Extract-ride_ext",
    description="ride ext table 을 만들고, 그 과정에서 필요한 테이블들을 2차 적재합니다.(검증 목적)",
    schedule_interval="0 */3 * * *", 
    default_args=default_args,
    catchup=False,
) as dag:

    check_ride_update = BigQueryCheckOperator(
        task_id = "check_ride_update",
        sql = utils.read_sql(
            os.path.join(os.environ["DAGS_FOLDER"], "sql/ride_ext/checker/check_ride_update.sql")
        ).format(execute_timestamp=execute_ts),
        gcp_conn_id = "google_cloud_for_tada",
        use_legacy_sql = False,
        on_failure_callback=task_fail_slack_alert,
    )

    check_to_run_ride_ext_eta = BigQueryCheckOperator(
        task_id = "check_to_run_ride_ext_eta",
        sql = utils.read_sql(
            os.path.join(os.environ["DAGS_FOLDER"], "sql/ride_ext/checker/check_servelog_to_run_ride_ext_eta.sql")
        ).format(execute_date=execute_date),
        gcp_conn_id = "google_cloud_for_tada",
        use_legacy_sql = False,
        on_failure_callback=task_fail_slack_alert,
    )

    extract_ride_ext_eta = BigQueryOperator(
        task_id = "extract_ride_ext_eta",
        sql = utils.read_sql(
            os.path.join(os.environ["DAGS_FOLDER"], "sql/ride_ext/ride_ext_eta.sql")
        ).format(execute_date=execute_date),
        bigquery_conn_id="google_cloud_for_tada",
        use_legacy_sql=False,
        destination_dataset_table=f"kr-co-vcnc-tada.tada_temp_london.ride_ext_eta${execute_date_formatted}",
        # destination_dataset_table=f"kr-co-vcnc-tada.tada_ext.ride_ext_eta${execute_date}",
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "DAY", "field": "date_kr"},
        on_failure_callback=task_fail_slack_alert,
    )

    extract_ride_ext_is_valid_1 = BigQueryOperator(
        task_id = "extract_ride_ext_is_valid_1",
        sql = utils.read_sql(
            os.path.join(os.environ["DAGS_FOLDER"], "sql/ride_ext/ride_ext_is_valid_1.sql")
        ).format(execute_date=execute_date),
        bigquery_conn_id="google_cloud_for_tada",
        use_legacy_sql=False,
        destination_dataset_table=f"kr-co-vcnc-tada.tada_temp_london.ride_ext_is_valid_1${execute_date_formatted}",
        # destination_dataset_table=f"kr-co-vcnc-tada.tada_ext.ride_ext_is_valid_1${execute_date}",
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "DAY", "field": "date_kr"},
        on_failure_callback=task_fail_slack_alert,
    )

    check_to_run_ride_ext_is_valid_2 = BigQueryCheckOperator(
        task_id = "check_to_run_ride_ext_is_valid_2",
        sql = utils.read_sql(
            os.path.join(os.environ["DAGS_FOLDER"], "sql/ride_ext/checker/check_to_run_ride_ext_is_valid_2.sql")
        ).format(execute_date=execute_date),
        gcp_conn_id = "google_cloud_for_tada",
        use_legacy_sql = False,
        on_failure_callback=task_fail_slack_alert,
    )

    extract_ride_ext_is_valid_2 = BigQueryOperator(
        task_id = "extract_ride_ext_is_valid_2",
        sql = utils.read_sql(
            os.path.join(os.environ["DAGS_FOLDER"], "sql/ride_ext/ride_ext_is_valid_2.sql")
        ).format(execute_date=execute_date),
        bigquery_conn_id="google_cloud_for_tada",
        use_legacy_sql=False,
        destination_dataset_table=f"kr-co-vcnc-tada.tada_temp_london.ride_ext_is_valid_2${execute_date_formatted}",
        # destination_dataset_table=f"kr-co-vcnc-tada.tada_ext.ride_ext_is_valid_2${execute_date}",
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "DAY", "field": "date_kr"},
        on_failure_callback=task_fail_slack_alert,
    )

    extract_ride_ext_is_valid = BigQueryOperator(
        task_id = "extract_ride_ext_is_valid",
        sql = utils.read_sql(
            os.path.join(os.environ["DAGS_FOLDER"], "sql/ride_ext/ride_ext_is_valid.sql")
        ).format(execute_date=execute_date),
        bigquery_conn_id="google_cloud_for_tada",
        use_legacy_sql=False,
        destination_dataset_table=f"kr-co-vcnc-tada.tada_temp_london.ride_ext_is_valid${execute_date_formatted}",
        # destination_dataset_table=f"kr-co-vcnc-tada.tada_ext.ride_ext_is_valid${execute_date}",
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "DAY", "field": "date_kr"},
        on_failure_callback=task_fail_slack_alert,
    )

    extract_ride_ext_ex = BigQueryOperator(
        task_id = "extract_ride_ext_ex",
        sql = utils.read_sql(
            os.path.join(os.environ["DAGS_FOLDER"], "sql/ride_ext/ride_ext_ex.sql")
        ).format(execute_date=execute_date),
        bigquery_conn_id="google_cloud_for_tada",
        use_legacy_sql=False,
        destination_dataset_table=f"kr-co-vcnc-tada.tada_temp_london.ride_ext_ex${execute_date_formatted}",
        # destination_dataset_table=f"kr-co-vcnc-tada.tada_ext.ride_ext_ex${execute_date}",
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "DAY", "field": "date_kr"},
        on_failure_callback=task_fail_slack_alert,
    )

    extract_ride_ext = BigQueryOperator(
        task_id = "extract_ride_ext",
        sql = utils.read_sql(
            os.path.join(os.environ["DAGS_FOLDER"], "sql/__ride_ext.sql")
        ).format(execute_date=execute_date),
        bigquery_conn_id="google_cloud_for_tada",
        use_legacy_sql=False,
        destination_dataset_table=f"kr-co-vcnc-tada.tada_temp_london.ride_ext${execute_date_formatted}",
        # destination_dataset_table=f"kr-co-vcnc-tada.tada_ext.ride_ext${execute_date}",
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "DAY", "field": "date_kr"},
        on_failure_callback=task_fail_slack_alert,
    )

    add_description_task = PythonOperator(
        task_id="description_task",
        python_callable=add_description.update_schema_description,
        op_kwargs={
            "table_fullname": "kr-co-vcnc-tada.tada_temp_london.ride_ext",
            # "table_fullname": "kr-co-vcnc-tada.tada_ext.ride_ext",
            "schema_description": schema_description,
        },
        on_failure_callback=task_fail_slack_alert,
    )

    task_done_checker_ride_ext_isvalid = DummyOperator(
        task_id="task_done_checker_ride_ext_isvalid",
        trigger_rule="all_success",
    )

    task_done_checker_ride_ext = DummyOperator(
        task_id="task_done_checker_ride_ext",
        trigger_rule="all_success",
    )


    #ride_ext_ex
    check_ride_update >> extract_ride_ext_ex >> task_done_checker_ride_ext >> extract_ride_ext >> add_description_task 
    #ride_ext_is_valid
    check_ride_update >> extract_ride_ext_is_valid_1 >> task_done_checker_ride_ext_isvalid >> extract_ride_ext_is_valid >> task_done_checker_ride_ext >> extract_ride_ext >> add_description_task
    check_ride_update >> check_to_run_ride_ext_is_valid_2 >> extract_ride_ext_is_valid_2 >> task_done_checker_ride_ext_isvalid >> extract_ride_ext_is_valid >> task_done_checker_ride_ext >> extract_ride_ext >> add_description_task
    #ride_ext_eta
    check_ride_update >> check_to_run_ride_ext_eta >> extract_ride_ext_eta >> task_done_checker_ride_ext >> extract_ride_ext >> add_description_task