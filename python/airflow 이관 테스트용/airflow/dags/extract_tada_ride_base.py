import os
from datetime import datetime, timedelta
from airflow import models
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
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
    "owner": "carrot",
    "depends_on_past": False,
    "start_date": datetime(2022, 4, 5),
    "email": ["carrot@socar.kr"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "project_id": "kr-co-vcnc-tada",
}

schema_description = [
    {"name": "date_kr", "description": "created_at_kr 기준 날짜"},
    {"name": "isoyear", "description": "created_at_kr 기준 ISO 연도"},
    {"name": "isoweek", "description": "created_at_kr 기준 ISO 주차"},
    {"name": "month", "description": "created_at_kr 기준 월"},
    {"name": "ride_id", "description": "호출 ID"},
    {
        "name": "call_view_type",
        "description": "호출화면타입 (21/01/05부터 적재) (LITE, PREMIUM, NXT, NEAR_TAXI, NXT_NUDGE, LITE_NUDGE, PREMIUM_NUDGE, NEAR_TAXI_NUDGE)",
    },
    {
        "name": "call_service_type",
        "description": "호출서비스타입 (LITE, PREMIUM, NXT, NEAR_TAXI)",
    },
    {
        "name": "determined_vehicle_type",
        "description": "매칭결정된 차량 타입 (LITE, PREMIUM, NXT, UNKNOWN)",
    },
    {"name": "type", "description": "ride 테이블의 type (LITE, PREMIUM, NXT)"},
    {
        "name": "status",
        "description": "호출 상태 (CANCELED, DROPPED_OFF, PENDING, ACCEPTED, ARRIVED, PICKED_UP)",
    },
    {"name": "is_passport_ride", "description": "패스포트 구독 유저의 호출"},
    {"name": "created_at_kr", "description": "호출시각 (KST)"},
    {"name": "accepted_at_kr", "description": "호출수락시각 (KST)"},
    {"name": "arrived_at_kr", "description": "차량의 고객위치 도착시각 (KST)"},
    {"name": "picked_up_at_kr", "description": "차량의 고객 픽업시각 (KST)"},
    {"name": "arrived_at_destination_at_kr", "description": "차량의 목적지 도착시각 (KST)"},
    {"name": "dropped_off_at_kr", "description": "고객하차시각 (KST)"},
    {"name": "cancelled_at_kr", "description": "호출취소시각 (KST)"},
    {"name": "origin_region", "description": "출발지 지역 (서울, 성남, 부산, 경기 등)"},
    {"name": "origin_lat", "description": "출발지 위도"},
    {"name": "origin_lng", "description": "출발지 경도"},
    {"name": "origin_name", "description": "출발지 이름"},
    {"name": "origin_address", "description": "출발지 주소"},
    {"name": "origin_h3_l7", "description": "출발지 H3 인덱스 (Level 7)"},
    {"name": "origin_h3_l9", "description": "출발지 H3 인덱스 (Level 9)"},
    {"name": "pickup_lat", "description": "픽업위치 위도"},
    {"name": "pickup_lng", "description": "픽업위치 경도"},
    {"name": "pickup_name", "description": "픽업위치 이름"},
    {"name": "pickup_address", "description": "픽업위치 주소"},
    {"name": "destination_region", "description": "목적지 지역 (서울, 성남, 부산, 경기 등)"},
    {"name": "destination_lat", "description": "목적지 위도"},
    {"name": "destination_lng", "description": "목적지 경도"},
    {"name": "destination_name", "description": "목적지 이름"},
    {"name": "destination_address", "description": "목적지 주소"},
    {"name": "destination_h3_l7", "description": "목적지 H3 인덱스 (Level 7)"},
    {"name": "destination_h3_l9", "description": "목적지 H3 인덱스 (Level 9)"},
    {"name": "dropoff_lat", "description": "하차위치 위도"},
    {"name": "dropoff_lng", "description": "하차위치 경도"},
    {"name": "dropoff_name", "description": "하차위치 이름"},
    {"name": "dropoff_address", "description": "하차위치 주소"},
    {
        "name": "cancellation_cause",
        "description": "취소 원인 (DISPATCH_TIMEOUT / DRIVER_CANCELLED / RIDER_CANCELLED)",
    },
    {"name": "cancellation_reason", "description": "고객 취소 사유"},
    {"name": "driver_cancellation_reason", "description": "드라이버 취소 사유"},
    {"name": "distance_meters", "description": "출발지-목적지 이동거리"},
    {"name": "adjusted_distance_meters", "description": ""},
    {"name": "estimation_distance_meters", "description": "출발지-목적지 예상이동거리"},
    {"name": "estimation_duration_seconds", "description": "출발지-목적지 예상이동시간"},
    {"name": "surge_percentage", "description": "서지 (100인 경우 NULL)"},
    {"name": "price_elasticity_testing_percentage", "description": ""},
    {"name": "estimation_min_cost", "description": "최소 예상 금액 (쿠폰 반영 후)"},
    {"name": "estimation_max_cost", "description": "최대 예상 금액 (쿠폰 반영 후)"},
    {"name": "estimation_original_min_cost", "description": "최소 예상 금액 (쿠폰 반영 전)"},
    {"name": "estimation_original_max_cost", "description": "최대 예상 금액 (쿠폰 반영 전)"},
    {"name": "coupon_id", "description": "쿠폰 ID"},
    {"name": "coupon_name", "description": "쿠폰 이름"},
    {"name": "system_coupon_tracking_identifier", "description": "쿠폰 identifier"},
    {"name": "payment_method_id", "description": "결제수단 ID"},
    {"name": "payment_profile_id", "description": ""},
    {
        "name": "card_type",
        "description": "결제 카드 종류 (CORPORATION : 법인 / PERSONAL : 개인 / NULL인 경우도 존재 (ex. 해외카드))",
    },
    {"name": "receipt_basic_fee", "description": "기본요금"},
    {"name": "receipt_distance_based_fee", "description": "거리요금"},
    {"name": "receipt_time_based_fee", "description": "시간요금"},
    {"name": "receipt_drive_fee", "description": "주행요금"},
    {"name": "receipt_discount_amount", "description": "할인금액 (쿠폰 등)"},
    {"name": "receipt_employee_discount_amount", "description": "직원할인금액"},
    {"name": "receipt_tollgate_fee", "description": "톨게이트 비용"},
    {"name": "receipt_total", "description": "결제 금액"},
    {"name": "receipt_refund_amount", "description": "환불 금액"},
    {"name": "receipt_extra", "description": "기타 금액"},
    {"name": "receipt_cancellation_fee", "description": "취소 수수료"},
    {"name": "receipt_tip_amount", "description": "팁 금액"},
    {"name": "credit_deposit_amount", "description": "적립된 크레딧 금액"},
    {"name": "credit_withdrawal_amount", "description": "사용한 크레딧 금액"},
    {"name": "biz_reason", "description": ""},
    {"name": "biz_reason_text", "description": ""},
    {
        "name": "origin_eta_multiplier",
        "description": "출발지 ETA 보정 배율. 서드 파티 API 호출 결과에 이 값을 곱해서 보정하여 유저에게 노출 시킵니다.",
    },
    {"name": "origin_eta_kr", "description": "출발지 도착 예정 시각. 최종 업데이트 값."},
    {"name": "destination_eta_kr", "description": "목적지 도착 예정 시각. 최종 업데이트 값."},
    {
        "name": "is_pure_session",
        "description": "순수수요세션 여부 (수요세션 동안 LITE, PREMIUM, NXT, NEAR_TAXI 중 하나의 타입으로만 호출했는지)",
    },
    {
        "name": "session_type",
        "description": "수요세션 타입 (LITE, PREMIUM, NXT, NEAR_TAXI, MIXED)",
    },
    {
        "name": "session_final_status",
        "description": "수요세션의 최종 상태 (수요세션의 마지막 호출의 status)",
    },
    {"name": "session_call_count", "description": "수요세션 동안 발생한 전체 호출건수"},
    {"name": "session_lite_count", "description": "수요세션 동안 발생한 라이트 호출건수"},
    {"name": "session_plus_count", "description": "수요세션 동안 발생한 플러스 호출건수"},
    {"name": "session_near_taxi_count", "description": "수요세션 동안 발생한 가까운타다 호출건수"},
    {
        "name": "session_start_at_kr",
        "description": "수요세션 시작시각 (수요세션의 첫번째 호출의 created_at_kr)",
    },
    {
        "name": "session_end_at_kr",
        "description": "수요세션 끝시각 (수요세션의 마지막 호출의 created_at_kr)",
    },
    {
        "name": "first_call_service_type",
        "description": "수요세션의 첫 호출의 호출서비스타입 (LITE, PREMIUM, NXT, NEAR_TAXI)",
    },
    {
        "name": "last_call_service_type",
        "description": "수요세션의 마지막 호출의 호출서비스타입 (LITE, PREMIUM, NXT, NEAR_TAXI)",
    },
    {"name": "demand_session_id", "description": "유저의 수요세션 아이디 (첫 탑승 이래 발생한 수요세션 순서)"},
    {"name": "is_valid_5_first", "description": "과거 유효호출 정의 5분 기준 첫번째 예약으로 정의한 유효호출"},
    {"name": "is_valid_20_first", "description": "과거 유효호출 정의 20분 기준 첫번째 예약으로 정의한 유효호출"},
    {"name": "is_valid_5_last", "description": "과거 유효호출 정의 5분 기준 마지막 예약으로 정의한 유효호출 "},
    {"name": "is_valid_20_last", "description": "과거 유효호출 정의 20분 기준 마지막 예약으로 정의한 유효호출"},
    {"name": "rider_id", "description": "유저 ID"},
    {"name": "sum_rating_by_driver_last_30", "description": "최근 30명의 드라이버에게 받은 평점"},
    {"name": "driver_id", "description": "드라이버 ID"},
    {"name": "driver_type", "description": "드라이버 타입 (LITE, PREMIUM, NXT)"},
    {"name": "is_individual_business_driver", "description": "개인 드라이버 여부"},
    {"name": "driver_agency_id", "description": "드라이버 운수사 ID"},
    {"name": "driver_agency_name", "description": "드라이버 운수사 이름"},
    {"name": "vehicle_id", "description": "차량 ID"},
    {"name": "vehicle_license_plate", "description": "차량 번호판"},
    {"name": "vehicle_region_type", "description": "차량 근무 지역 (SEOUL, SEOUGNAM, BUSAN)"},
    {"name": "rating_by_user", "description": "유저 -> 드라이버 평가"},
    {"name": "rating_by_user_reason", "description": ""},
    {"name": "rating_by_user_created_at_kr", "description": ""},
    {"name": "rating_by_user_ignored_at_kr", "description": ""},
    {"name": "rating_by_user_tags", "description": ""},
    {"name": "rating_by_driver", "description": "드라이버 -> 유저 평가"},
    {"name": "rating_by_driver_reason", "description": ""},
    {"name": "rating_by_driver_created_at_kr", "description": ""},
    {"name": "passport_source", "description": "패스포트 가입경로 (SOCAR, VCNC)"},
    {"name": "passport_product_identifier", "description": "패스포트 가입 상품"},
    {
        "name": "is_app_meter",
        "description": "앱미터기 적용 유무(status가 ACCEPTED, ARRIVED, PICKED_UP, DROPPED_OFF일 때 존재)",
    },
    {"name": "is_reservation", "description": "호출예약 생성 ride 구분자"},
]

with models.DAG(
    dag_id="Extract-ride-base",
    description="Extract ride_base",
    schedule_interval="10 * * * *",
    catchup=False,
    default_args=default_args,
) as dag:
    query_task = BigQueryOperator(
        dag=dag,
        bigquery_conn_id="google_cloud_for_tada",
        task_id=f"extract_ride_base",
        sql=utils.read_sql(
            os.path.join(os.environ["DAGS_FOLDER"], "sql/ride_base.sql")
        ),
        use_legacy_sql=False,
        destination_dataset_table=f"kr-co-vcnc-tada.tada_store.ride_base",
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "DAY", "field": "date_kr"},
        on_failure_callback=task_fail_slack_alert,
    )

    add_description_task = PythonOperator(
        task_id="description_task",
        python_callable=add_description.update_schema_description,
        op_kwargs={
            "table_fullname": "kr-co-vcnc-tada.tada_store.ride_base",
            "schema_description": schema_description,
        },
        dag=dag,
        on_failure_callback=task_fail_slack_alert,
    )

    query_task >> add_description_task
