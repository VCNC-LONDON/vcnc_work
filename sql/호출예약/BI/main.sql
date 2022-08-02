WITH 

date_dummy AS (
    SELECT 
        d AS date_kr
    FROM 
        UNNEST(GENERATE_DATE_ARRAY("2022-01-12", CURRENT_DATE("Asia/Seoul"), INTERVAL 1 DAY)) AS d
),

reservation_by_reserve AS (
    SELECT 
        reserve_date_kr,
        ride_type,
        COUNT(DISTINCT reservation_id) AS reserve_cnt,
        COUNT(DISTINCT user_id) AS rider_cnt,
        AVG(TIMESTAMP_DIFF(expected_pick_up_at, created_at, SECOND)/60) AS leadtime_min
    FROM 
        `kr-co-vcnc-tada.tada_reservation.reservation_base`
    GROUP BY 
        reserve_date_kr, ride_type
),

reservation_by_ride_base AS (
    SELECT 
        ride_date_kr,
        ride_type,
        COUNT(DISTINCT reservation_id) AS ride_reserve_cnt,
        COUNT(DISTINCT ride_id) AS match_reserve_cnt,
        COUNT(DISTINCT IF(ride_status = "DROPPED_OFF", reservation_id, NULL)) AS done_reserve_cnt,
        COUNT(DISTINCT IF(cancellation_cause = "DISPATCH_TIMEOUT", reservation_id, NULL)) AS not_match_reserve_cnt,
        COUNT(DISTINCT IF(cancellation_cause = "RIDER_CANCELLED" , reservation_id, NULL)) AS rider_cancel_reserve_cnt,
        COUNT(DISTINCT IF(cancellation_cause = "RIDER_CANCELLED" AND (accepted_at IS NULL OR accepted_at > cancel_at), reservation_id, NULL)) AS rider_cancel_before_accept_reserve_cnt,
        COUNT(DISTINCT IF(cancellation_cause = "RIDER_CANCELLED" AND accepted_at <= cancel_at, reservation_id, NULL)) AS rider_cancel_after_accept_reserve_cnt,
        COUNT(DISTINCT IF(cancellation_cause = "DRIVER_CANCELLED", reservation_id, NULL)) AS driver_cancel_reserve_cnt,
        AVG(surge) AS avg_surge,
        SUM(ride_receipt_total + ride_receipt_discount_amount + ride_receipt_employee_discount_amount + cancellation_fee) AS revenue,
        SUM(ride_receipt_total + cancellation_fee) AS net_revenue,
    FROM 
        `kr-co-vcnc-tada.tada_reservation.reservation_base`
    GROUP BY 
        ride_date_kr, ride_type
),

reservation_by_reserve_most_rider_pick_time AS (
    WITH 
    most_call AS (
        SELECT 
            reserve_date_kr,
            ride_type,
            ride_hour,
            ROW_NUMBER() OVER (PARTITION BY reserve_date_kr, ride_type ORDER BY ride_reserve_cnt DESC) AS rn
        FROM (
            SELECT
                reserve_date_kr,
                ride_type,
                EXTRACT(HOUR FROM DATETIME(expected_pick_up_at, "Asia/Seoul")) AS ride_hour,
                COUNT(DISTINCT reservation_id) AS ride_reserve_cnt, 
            FROM
                `kr-co-vcnc-tada.tada_reservation.reservation_base`
            GROUP BY 
                reserve_date_kr, ride_type, ride_hour
        )
    )

    SELECT 
        reserve_date_kr,
        ride_type,
        MAX(IF(rn=1, ride_hour, NULL)) AS m1_hour,
        MAX(IF(rn=2, ride_hour, NULL)) AS m2_hour,
        MAX(IF(rn=3, ride_hour, NULL)) AS m3_hour,
    FROM 
        most_call
    GROUP BY 
        reserve_date_kr, ride_type
),

reservation_by_ride_most_driver_pick_time AS (
    WITH 
    most_call AS (
        SELECT 
            ride_date_kr,
            ride_type,
            ride_hour,
            ROW_NUMBER() OVER (PARTITION BY ride_date_kr, ride_type ORDER BY ride_reserve_cnt DESC) AS rn
        FROM (
            SELECT
                ride_date_kr,
                ride_type,
                EXTRACT(HOUR FROM DATETIME(expected_pick_up_at, "Asia/Seoul")) AS ride_hour,
                COUNT(DISTINCT reservation_id) AS ride_reserve_cnt, 
            FROM
                `kr-co-vcnc-tada.tada_reservation.reservation_base`
            WHERE
                accepted_at IS NOT NULL
            GROUP BY 
                ride_date_kr, ride_type, ride_hour
        )
    )

    SELECT 
        ride_date_kr,
        ride_type,
        MAX(IF(rn=1, ride_hour, NULL)) AS m1_hour,
        MAX(IF(rn=2, ride_hour, NULL)) AS m2_hour,
        MAX(IF(rn=3, ride_hour, NULL)) AS m3_hour,
    FROM 
        most_call
    GROUP BY 
        ride_date_kr, ride_type
),

driver_activity AS (
    WITH 

    driver_activity_raw AS (
        SELECT
            da.date_kr,
            da.driver_id,
            da.vehicle_id,
            da.seq_id,
            DATETIME(da.start_at, 'Asia/Seoul') activity_start_dt,
            DATETIME(da.end_at, 'Asia/Seoul') activity_end_dt,
            TIMESTAMP_DIFF(da.end_at, da.start_at, SECOND) / 60 activity_duration_minute,
            da.activity_status,
            da.ride_status,
            r.status,
            da.ride_id,
            DATETIME(IFNULL(IFNULL(rr.prestarted_at, rr.start_at), MIN(IFNULL(rr.prestarted_at, rr.start_at)) OVER (PARTITION BY da.driver_id ORDER BY da.seq_id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)),"Asia/Seoul") reserve_seq_start_dt, #출근~퇴근까지 그룹화
            r.receipt_total + r.receipt_employee_discount_amount + r.receipt_discount_amount AS revenue,
            r.receipt_total AS net_revenue,
        FROM `kr-co-vcnc-tada.tada.driver_activity` AS da
        LEFT JOIN `kr-co-vcnc-tada.tada.ride` AS r
        ON da.ride_id = r.id
        LEFT JOIN tada_reservation.reservation_base AS rr
        ON da.ride_id = rr.ride_id
        WHERE da.date_kr >= '2022-01-01'
        AND SUBSTR(da.driver_id, 1, 3) != 'GIG' # 대리 드라이버 제외
    ),

    driver_activity_sessionize AS (
        SELECT
            * EXCEPT (session_id),
            IF(reserve_seq_start_dt IS NOT NULL, True, False) AS is_reserve_work,
            IFNULL(session_id, MAX(session_id) OVER (PARTITION BY driver_id ORDER BY seq_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) session_id #출근~퇴근까지 그룹화
        FROM (
            SELECT 
                * EXCEPT(reserve_seq_start_dt),
                (CASE 
                    WHEN activity_status IN ('OFF', 'DISPATCHING', 'IDLE') AND activity_duration_minute >= 120 THEN seq_id # 상태값이 정해진 상태로 두시간이상 지속되는 경우 세션화
                    ELSE NULL 
                END) session_id,
                IF(activity_end_dt >= reserve_seq_start_dt, reserve_seq_start_dt, NULL) AS reserve_seq_start_dt
            FROM driver_activity_raw
        )
        ORDER BY activity_start_dt
    ),

    driver_activity_base AS (
        SELECT 
            date_kr,
            driver_id,
            vehicle_id,
            session_id,
            seq_id,
            activity_start_dt,
            activity_end_dt,
            FIRST_VALUE(activity_start_dt) OVER (PARTITION BY driver_id, session_id ORDER BY seq_id) AS activity_session_start_dt,
            FIRST_VALUE(activity_end_dt) OVER (PARTITION BY driver_id, session_id ORDER BY seq_id DESC) AS activity_session_end_dt,
            activity_duration_minute,
            (CASE
                WHEN  is_reserve_work AND activity_status IN ("IDLE" , "OFF") THEN activity_duration_minute
                WHEN  is_reserve_work AND activity_start_dt <= reserve_seq_start_dt THEN DATETIME_DIFF(activity_end_dt, reserve_seq_start_dt, SECOND)/60
                WHEN  is_reserve_work AND activity_start_dt > reserve_seq_start_dt THEN activity_duration_minute
            END) AS activity_duration_minute_reserve,
            activity_status,
            is_reserve_work,
            ride_status,
            status,
            ride_id,
            reserve_seq_start_dt,
            revenue,
            net_revenue
        FROM driver_activity_sessionize 
        -- WHERE driver_id = "DNX00670" 
        -- WHERE dirver_id = "DDA98248"
        -- WHERE activity_status != "OFF" 
    ),

    working_hour AS (
        SELECT 
            DATE(activity_session_start_dt) AS session_start_date_kr,
            COUNT(DISTINCT driver_id) AS driver_cnt,
            SUM(activity_duration_minute) AS session_duration_minute,
            SUM(IF(activity_status IN ("DISPATCHING", "RIDING"), activity_duration_minute, NULL)) AS work_session_duration_minute,
            SUM(IF(activity_status IN ("DISPATCHING", "RIDING"), activity_duration_minute_reserve, NULL)) AS reserve_work_session_duration_minute,
        FROM driver_activity_base
        GROUP BY 1
    ),

    working_rev AS (
        SELECT 
            DATE(activity_session_start_dt) AS session_start_date_kr,
            SUM(revenue) AS rev,
            SUM(IF(NOT is_reserve_work , revenue, 0 )) AS realtime_rev,
            SUM(IF(is_reserve_work , revenue, 0 )) AS reserve_rev,
            SUM(net_revenue) AS net_rev,
            SUM(IF(NOT is_reserve_work , net_revenue, 0 )) AS realtime_net_rev,
            SUM(IF(is_reserve_work , net_revenue, 0 )) AS reserve_net_rev,
        FROM (
            SELECT
                DISTINCT driver_id,
                session_id,
                activity_session_start_dt,
                activity_session_end_dt,
                activity_status,
                is_reserve_work,
                ride_id,
                status,
                revenue,
                net_revenue
            FROM driver_activity_base
        )
        GROUP BY 1
    )
    SELECT
    FROM
)


select * from reservation_by_ride_most_driver_pick_time