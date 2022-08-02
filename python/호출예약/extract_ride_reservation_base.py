from datetime import datetime, timedelta
from airflow import models
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from dependencies import slack_operator, add_description
from airflow.operators.dummy_operator import DummyOperator


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
    'owner': 'london',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 17),
    'email': ['london@vcnc.co.kr'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_exponential_backoff': True,
    'retry_delay': timedelta(minutes=5),
    'max_retry_delay': timedelta(minutes=20),
    'project_id': 'kr-co-vcnc-tada',
}

reservation_base_query = """
WITH 

reservation_acceptance AS (
    SELECT
        DISTINCT ride_reservation_id AS reservation_id,
        FIRST_VALUE(driver_id) OVER (PARTITION BY ride_reservation_id ORDER BY created_at DESC) AS last_accepted_driver_id,
        FIRST_VALUE(created_at) OVER (PARTITION BY ride_reservation_id ORDER BY created_at DESC) AS last_accepted_at,
        COUNT(DISTINCT driver_id) OVER (PARTITION BY ride_reservation_id) AS accept_cnt,
    FROM `kr-co-vcnc-tada.tada.ride_reservation_acceptance`
),

reservation AS (
    SELECT
        DATE(rr.created_at, "Asia/Seoul") AS reserve_date_kr,
        DATE(rr.expected_pick_up_at, "Asia/Seoul") AS ride_date_kr,
        rr.reservation_group_id,
        rr.id AS reservation_id, 
        rr.ride_type, 
        rr.status AS reservation_status, 
        IFNULL(ra.accept_cnt,0) AS accept_cnt,
        rr.user_id AS rider_id, 
        rr.accept_expiry, 
        rr.expected_pick_up_at, 
        rr.created_at AS created_at, 
        rr.accepted_at AS accepted_at,
        rr.prestarted_at AS prestarted_at, 
        rr.started_at AS start_at,
        rr.cancelled_at AS cancel_at, 
        DATETIME(rr.accept_expiry, "Asia/Seoul") AS accept_expiry_kr,
        DATETIME(rr.expected_pick_up_at, "Asia/Seoul") AS expected_pick_up_at_kr, 
        DATETIME(rr.created_at, "Asia/Seoul") AS created_at_kr, 
        DATETIME(rr.accepted_at, "Asia/Seoul") AS accepted_at_kr,
        DATETIME(rr.prestarted_at, "Asia/Seoul") AS prestarted_at_kr, 
        DATETIME(rr.started_at, "Asia/Seoul") AS start_at_kr,
        DATETIME(rr.cancelled_at, "Asia/Seoul") AS cancel_at_kr, 
        ra.last_accepted_driver_id,
        ra.last_accepted_at,
        rr.driver_id, 
        rr.taxi_region_type, 
        rr.ride_id, 
        r.status AS ride_status,
        r.created_at AS ride_created_at,
        r.accepted_at AS ride_accepted_at,
        r.arrived_at AS ride_arrived_at,
        r.picked_up_at AS ride_pick_up_at,
        r.dropped_off_at As ride_drop_off_at,
        r.cancelled_at AS ride_cancel_at,
        DATETIME(r.created_at,"Asia/Seoul") AS ride_created_at_kr,
        DATETIME(r.accepted_at, "Asia/Seoul") AS ride_accepted_at_kr,
        DATETIME(r.arrived_at, "Asia/Seoul") AS ride_arrived_at_kr,
        DATETIME(r.picked_up_at, "Asia/Seoul") AS ride_pick_up_at_kr,
        DATETIME(r.dropped_off_at, "Asia/Seoul") As ride_drop_off_at_kr,
        DATETIME(r.cancelled_at, "Asia/Seoul") AS ride_cancel_at_kr,
        TRIM(CONCAT(IFNULL(JSON_VALUE(rr.ex_data, "$.originAddressDetail.siDo"),""), " ", IFNULL(JSON_VALUE(rr.ex_data, "$.originAddressDetail.siGunGu"),""), " ", IFNULL(JSON_VALUE(rr.ex_data, "$.originAddressDetail.eupMyeonDong"),""), " ", IFNULL(JSON_VALUE(rr.ex_data, "$.originAddressDetail.beonji"),""))) AS origin_address,
        JSON_VALUE(rr.ex_data, "$.originAddressDetail.poiName") AS origin_name,
        IFNULL(JSON_VALUE(rr.ex_data, "$.originAddressDetail.siDo"),"") AS origin_sido,
        IFNULL(JSON_VALUE(rr.ex_data, "$.originAddressDetail.siGunGu"),"") AS origin_sgg,
        IFNULL(JSON_VALUE(rr.ex_data, "$.originAddressDetail.eupMyeonDong"),"") AS origin_emd,
        rr.origin_lat AS start_lat,
        rr.origin_lng AS start_lng,
        TRIM(CONCAT(IFNULL(JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.siDo"),""), " ", IFNULL(JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.siGunGu"),""), " ", IFNULL(JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.eupMyeonDong"),""), " ", IFNULL(JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.beonji"),""))) AS destination_address,
        JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.poiName") AS destination_name,
        IFNULL(JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.siDo"),"") AS destination_sido,
        IFNULL(JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.siGunGu"),"") AS destination_sgg,
        IFNULL(JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.eupMyeonDong"),"") AS destination_emd,
        rr.destination_lat,
        rr.destination_lng,
        r.dropoff_address,
        r.dropoff_name,
        r.dropoff_lat,
        r.dropoff_lng,
        CAST(JSON_VALUE(rr.estimation, "$.distanceMeters") AS int64) AS estimate_distance,
        r.distance_meters AS ride_distance,
        CAST(JSON_VALUE(rr.estimation, "$.surgePercentage") AS int64) AS surge,
        CAST(JSON_VALUE(rr.estimation, "$.pureFareAmount") AS int64) AS estimate_pure_amount,
        CAST(JSON_VALUE(rr.estimation, "$.surchargedAmount") AS int64) AS estimate_surge_amount,
        CAST(JSON_VALUE(rr.estimation, "$.rideFee") AS int64) AS estimate_ride_fee,
        CAST(JSON_VALUE(rr.estimation, "$.reservationFee") AS int64) AS estimate_reserve_fee,
        CAST(JSON_VALUE(rr.estimation, "$.totalFee") AS int64) AS estimate_total_fee,
        IFNULL(r.receipt_total,0) AS ride_revenue,
        IFNULL(r.receipt_discount_amount,0) AS ride_discount,
        IFNULL(r.receipt_employee_discount_amount,0) AS ride_employee_discount,
        IFNULL(r.receipt_cancellation_fee,0) AS ride_cancellation_fee,
        rr.cancellation_cause AS cancellation_cause, 
        r.cancellation_cause AS ride_cancellation_cause,
        IFNULL(rr.cancellation_fee_type, "NO_CANCELLATION_FEE") AS cancellation_fee_type,
        IFNULL(CAST(JSON_VALUE(rr.receipt, "$.cancellationFee") AS int64),0) AS cancellation_fee_by_driver,
        IF(r.cancellation_cause = "RIDER_CANCELLED" AND rr.cancellation_fee_type = "CANCELLATION_FEE", r.receipt_cancellation_fee, 0) AS cancellation_fee_by_rider,
        rr.estimation AS reservation_estimation_data,
        rr.ex_data AS reservation_ex_data,
    FROM `kr-co-vcnc-tada.tada.ride_reservation` AS rr
    LEFT JOIN `kr-co-vcnc-tada.tada.ride` AS r 
    ON rr.ride_id = r.id
    LEFT JOIN reservation_acceptance AS ra 
    ON ra.reservation_id = rr.id
)

SELECT * FROM reservation WHERE reserve_date_kr BETWEEN "{run_date}" - 15 AND "{run_date}"
"""


schema_description = [
    {'name' : 'reserve_date_kr', 'description': '예약 접수일자'},
    {'name' : 'ride_date_kr', 'description': '탑승 예정일자'},
    {'name' : 'reservation_group_id', 'description':'1개 이상 예약을 동시에 생성하는 경우 group id 로 구분할 수 있음'},
    {'name' : 'reservation_id', 'description': '예약 1건당 개별로 생성되는 id'},
    {'name' : 'ride_type', 'description': '예약 차량 타입'},
    {'name' : 'reservation_status', 'description': '예약 상태(시점마다 업데이트될 수 있음)'},
    {'name' : 'accept_cnt', 'description': '누적 수락 횟수(탑승 전까지 여러번 수락될 수 있음)'},
    {'name' : 'rider_id', 'description': '호출 유저 id'},
    {'name' : 'accept_expiry', 'description': '미수락시 자동 취소 되는 기준 시간'},
    {'name' : 'expected_pick_up_at', 'description': '탑승 예정 시간'},
    {'name' : 'created_at', 'description': '예약 생성 시간'},
    {'name' : 'accepted_at', 'description': '예약 수락 시간(최신)'},
    {'name' : 'prestarted_at', 'description': '드라이버가 출발지로 이동 버튼 클릭 시간'},
    {'name' : 'start_at', 'description': '고객에게 도착 완료 알림 또는 탑승 예정 시간(ride_id가 생성되는 시점)'},
    {'name' : 'cancel_at', 'description': '호출예약 취소 시간'},
    {'name' : 'accept_expiry_kr', 'description': '미수락시 자동 취소 되는 기준 시간(KTC)'},
    {'name' : 'expected_pick_up_at_kr', 'description': '탑승 예정 시간(KTC)'},
    {'name' : 'created_at_kr', 'description': '예약 생성 시간(KTC)'},
    {'name' : 'accepted_at_kr', 'description': '예약 수락 시간(최신)(KTC)'},
    {'name' : 'prestarted_at_kr', 'description': '드라이버가 출발지로 이동 버튼 클릭 시간(KTC)'},
    {'name' : 'start_at_kr', 'description': '고객에게 도착 완료 알림 또는 탑승 예정 시간(KTC)'},
    {'name' : 'cancel_at_kr', 'description': '호출예약 취소 시간(KTC)'},
    {'name' : 'last_accepted_driver_id', 'description': '마지막으로 수락한 드라이버 id'},
    {'name' : 'last_accepted_at', 'description': '마지막으로 수락된 시간'},
    {'name' : 'driver_id', 'description': '운행이 결정된 드라이버 id'},
    {'name' : 'taxi_region_type', 'description': '운행 차량의 등록지역'},
    {'name' : 'ride_id', 'description': 'start_at 시점에 생성되는 ride_id(tada.ride.id 와 join key)'},
    {'name' : 'ride_status', 'description': 'ride 상태'},
    {'name' : 'ride_created_at', 'description': 'ride 생성 시간(=start_at)'},
    {'name' : 'ride_accepted_at', 'description': 'ride 수락 시간(=start_at)'},
    {'name' : 'ride_arrived_at', 'description': '출발지 도착 시간'},
    {'name' : 'ride_pick_up_at', 'description': '유저 탑승 시간'},
    {'name' : 'ride_drop_off_at', 'description': '유저 하차 시간'},
    {'name' : 'ride_cancel_at', 'description': 'ride 취소 시간'},
    {'name' : 'ride_created_at_kr', 'description': 'ride 생성 시간(KTC)(=start_at_kr)'},
    {'name' : 'ride_accepted_at_kr', 'description': 'ride 수락 시간(KTC)(=start_at_kr)'},
    {'name' : 'ride_arrived_at_kr', 'description': '출발지 도착 시간(KTC)'},
    {'name' : 'ride_pick_up_at_kr', 'description': '유저 탑승 시간(KTC)'},
    {'name' : 'ride_drop_off_at_kr', 'description': '유저 하차 시간(KTC)'},
    {'name' : 'ride_cancel_at_kr', 'description': 'ride 취소 시간(KTC)'},
    {'name' : 'origin_address', 'description': '탑승예정지 주소'},
    {'name' : 'origin_name', 'description': '탑승예정지 이름'},
    {'name' : 'origin_sido', 'description': '탑승예정지 시도'},
    {'name' : 'origin_sgg', 'description': '탑승예정지 시군구'},
    {'name' : 'origin_emd', 'description': '탑승예정지 읍면동'},
    {'name' : 'start_lat', 'description': '탑승예정지 위도'},
    {'name' : 'start_lng', 'description': '탑승예정지 경도'},
    {'name' : 'destination_address', 'description': '목적지 주소'},
    {'name' : 'destination_name', 'description': '목적지 이름'},
    {'name' : 'destination_sido', 'description': '목적지 시도'},
    {'name' : 'destination_sgg', 'description': '목적지 시군구'},
    {'name' : 'destination_emd', 'description': '목적지 읍면동'},
    {'name' : 'destination_lat', 'description' : '목적지 위도'},
    {'name' : 'destination_lng', 'description' : '목적지 경도'},
    {'name' : 'dropoff_address', 'description' : '실 하차지 주소'},
    {'name' : 'dropoff_name', 'description' : '실 하차지 이름'},
    {'name' : 'dropoff_lat', 'description' : '실 하차지 위도'},
    {'name' : 'dropoff_lng', 'description' : '실 하차지 경도'},
    {'name' : 'estimate_distance', 'description' : '예상 이동거리'},
    {'name' : 'ride_distance', 'description' : '실제 이동거리'},
    {'name' : 'surge', 'description' : '서지'},
    {'name' : 'estimate_pure_amount', 'description' : '최초 요금'},
    {'name' : 'estimate_surge_amount', 'description' : '서지 요금'},
    {'name' : 'estimate_ride_fee', 'description' : '예상 주행 요금'},
    {'name' : 'estimate_reserve_fee', 'description' : '예상 예약 요금'},
    {'name' : 'estimate_total_fee', 'description' : '예상 최종 요금'},
    {'name' : 'ride_revenue', 'description' : '결제액(=ride.ride_recipt_total)'},
    {'name' : 'ride_discount', 'description' : '할인액(=ride.ride_discount_amount)'},
    {'name' : 'ride_employee_discount', 'description' : '직원 할인액(=ride.ride_employee_discount_amount)'},
    {'name' : 'ride_cancellation_fee', 'description' : '운행 취소 수수료'},
    {'name' : 'cancellation_cause', 'description' : '예약 취소 귀책'},
    {'name' : 'ride_cancellation_cause', 'description' : '운행 취소 귀책'},
    {'name' : 'cancellation_fee_type', 'description' : '예약 취소 수수료 구분'},
    {'name' : 'cancellation_fee_by_driver', 'description' : '드라이버 취소 수수료'},
    {'name' : 'cancellation_fee_by_rider', 'description' : '유저 취소 수수료'},
    {'name' : 'reservation_estimation_data', 'description' : '호출예약 비용 예산 raw(json)'},
    {'name' : 'reservation_ex_data', 'description' : '호출예약 정보 raw(json)'}
]

date_suffix = ["{{ macros.ds_add(ds, -0) }}", "{{ macros.ds_add(ds, -1) }}"]

date_suffix_fromatted = [
    "{{ macros.ds_format(macros.ds_add(ds, -0), '%Y-%m-%d', '%Y%m%d') }}",
    "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d') }}"
]


with models.DAG(
        dag_id='Extract-ride_reservation_base',
        description='extract ride reservaton base ',
        schedule_interval='0 */4 * * *',
        default_args=default_args,
        catchup=True
        ) as dag:

    combine_task = DummyOperator(
        task_id='combine_tasks',
        trigger_rule='all_success',
        dag=dag,
    )

    for i in range(0, 2):
        rule = ['before_one_day', 'before_two_day']

        extract_ride_reservation_base_task = BigQueryOperator(
            dag=dag,
            bigquery_conn_id='google_cloud_for_tada',
            task_id=f"extract_ride_reservation_base_{rule[i]}",
            sql=reservation_base_query,
            use_legacy_sql=False,
            destination_dataset_table=f'kr-co-vcnc-tada.tada_reservation.reservation_base${date_suffix_fromatted[i]}',
            write_disposition='WRITE_TRUNCATE',
            time_partitioning={'type': 'DAY', 'field': 'reserve_date_kr'},
            cluster_fields = ['reserve_date_kr', 'ride_date_kr'],
            on_failure_callback=task_fail_slack_alert,
        )

        extract_ride_reservation_base_task >> combine_task

    add_description_task = PythonOperator(
        dag=dag,
        task_id='description_task',
        python_callable=add_description.update_schema_description,
        op_kwargs={'table_fullname': 'kr-co-vcnc-tada.tada_reservation.reservation_base',
                   'schema_description': schema_description},
        on_failure_callback=task_fail_slack_alert
    )

    combine_task >> add_description_task

