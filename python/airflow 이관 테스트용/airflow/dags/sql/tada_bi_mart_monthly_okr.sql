WITH ride_base AS (
  SELECT
    FORMAT_DATE('%Y-%m', date_kr) AS year_month,
    COUNT(DISTINCT IF(NOT dropped_off_at_kr IS NULL, rider_id, NULL)) AS customer_cnt,
    SAFE_CAST(AVG(IF(NOT origin_eta_kr IS NULL AND NOT arrived_at_kr IS NULL, unix_seconds(timestamp(origin_eta_kr)) - unix_seconds(timestamp(accepted_at_kr)), NULL)) AS NUMERIC) AS avg_eta_secs,
    SAFE_CAST(AVG(IF(NOT arrived_at_kr IS NULL AND NOT arrived_at_kr IS NULL, unix_seconds(timestamp(arrived_at_kr)) - unix_seconds(timestamp(accepted_at_kr)), NULL)) AS NUMERIC) AS avg_ata_secs

  FROM
    tada_store.ride_base

  WHERE
    1=1
    and date_kr BETWEEN DATE_SUB(LAST_DAY("{target_date}", MONTH) + 1, INTERVAL 1 MONTH) AND LAST_DAY("{target_date}", MONTH)

  GROUP BY
    year_month
),

driver_activity_ext AS (
  SELECT
    da.date_kr,
    da.driver_id,
    d.is_individual_business is_individual_business_driver, 
    da.vehicle_id,
    v.taxi_region_type vehicle_region_type,
    CASE WHEN v.taxi_region_type = 'SEOUL' THEN '서울' 
      WHEN v.taxi_region_type = 'BUSAN' THEN '부산' 
      WHEN v.taxi_region_type = 'SEONGNAM' THEN '성남' 
      ELSE v.taxi_region_type END AS service_region, 
    da.seq_id,
    DATETIME(da.start_at, 'Asia/Seoul') AS activity_start_at_kr,
    DATETIME(da.end_at, 'Asia/Seoul') AS activity_end_at_kr,
    TIMESTAMP_DIFF(da.end_at, da.start_at, SECOND) / 60 AS activity_duration_minute,
    da.activity_status,
    da.ride_status,
    da.ride_id,
    r.status,
    IF(ride_status = 'ARRIVED' AND r.status = 'DROPPED_OFF',IFNULL(r.receipt_drive_fee, 0), 0) + IF(ride_status = 'ARRIVED' AND r.status = 'DROPPED_OFF',IFNULL(r.receipt_tollgate_fee, 0), 0) + IF(ride_status = 'ACCEPTED' AND r.status = 'CANCELED', IFNULL(r.receipt_cancellation_fee, 0), 0) AS revenue,
    IF(ride_status = 'ARRIVED' AND r.status = 'DROPPED_OFF',IFNULL(r.receipt_total, 0), 0) + IF(ride_status = 'ACCEPTED' AND r.status = 'CANCELED',IFNULL(r.receipt_total, 0), 0) AS net_revenue,
    da.trip_status
  
  FROM tada.driver_activity AS da
    LEFT JOIN tada.driver AS d
      ON da.driver_id = d.id
    LEFT JOIN tada.vehicle AS v
      ON da.vehicle_id = v.id
    LEFT JOIN tada.ride AS r
      ON da.ride_id = r.id
  
  WHERE
    1=1
    AND da.date_kr BETWEEN DATE_SUB(LAST_DAY("{target_date}", MONTH) + 1, INTERVAL 1 MONTH) AND LAST_DAY("{target_date}", MONTH)
    AND SUBSTR(da.driver_id, 1, 3) NOT IN ('GIG', 'DVC')
),

session_start_marked AS (
  SELECT
    *,
    CASE WHEN is_off_session IS NOT NULL THEN TRUE
      WHEN LAG(is_off_session) OVER (PARTITION BY driver_id ORDER BY seq_id) IS NOT NULL THEN TRUE
      WHEN vehicle_id != last_used_vehicle_id THEN TRUE
      ELSE NULL END AS is_session_start_activity
  
  FROM
    (
      SELECT 
        *,
        IF(vehicle_id IS NOT NULL, LAST_VALUE(vehicle_id IGNORE NULLS) OVER (PARTITION BY driver_id ORDER BY seq_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), NULL) AS last_used_vehicle_id,
        CASE WHEN seq_id = 0 AND activity_status = 'OFF' THEN TRUE
          WHEN activity_status IN ('OFF', 'DISPATCHING', 'IDLE') AND activity_duration_minute >= 120 THEN TRUE
          ELSE NULL END AS is_off_session
    
      FROM
        driver_activity_ext
    )
),

session_marked AS (
  SELECT 
    * EXCEPT (session_id),
    IFNULL(session_id, MAX(session_id) OVER (PARTITION BY driver_id ORDER BY seq_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS session_id,
    IF(is_off_session, 'OFF', 'WORKING') AS session_status
  
  FROM
    (
      SELECT 
        *,
        IF(is_session_start_activity, ROW_NUMBER() OVER (PARTITION BY driver_id, is_session_start_activity ORDER BY seq_id), NULL) AS session_id
      
      FROM
        session_start_marked
    )
),

vehicle_type AS (
  SELECT
    DISTINCT vehicle_id,
    IF(vehicle_id = 'VSMF592T2TDMJ6PG', 'PREMIUM', LAST_VALUE(type) OVER (PARTITION BY vehicle_id ORDER BY created_at)) AS vehicle_ride_type

  FROM
    tada.ride
  
  WHERE
    1=1
    AND date_kr BETWEEN DATE_SUB(LAST_DAY("{target_date}", MONTH) + 1, INTERVAL 1 MONTH) AND LAST_DAY("{target_date}", MONTH)
    AND type in ('LITE', 'PREMIUM', 'NXT')
    AND NOT vehicle_id IS NULL
),

final AS (
  SELECT 
    sm.*,
    vt.vehicle_ride_type

  FROM
    (
      SELECT
        * EXCEPT (vehicle_id, last_used_vehicle_id, is_off_session, is_session_start_activity),
        IFNULL(vehicle_id, IF(session_status = 'OFF', NULL, LAST_VALUE(vehicle_id IGNORE NULLS) OVER (PARTITION BY driver_id, session_id ORDER BY seq_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING))) AS vehicle_id,
        MIN(date_kr) OVER (PARTITION BY driver_id, session_id, session_status) AS session_start_date_kr,
        MIN(activity_start_at_kr) OVER (PARTITION BY driver_id, session_id, session_status) AS session_start_at_kr,
        MAX(activity_end_at_kr) OVER (PARTITION BY driver_id, session_id, session_status) AS session_end_at_kr,
        COUNT(DISTINCT ride_id) OVER (PARTITION BY driver_id, session_id, session_status) AS session_ride_count,
        COUNT(DISTINCT IF(status = 'DROPPED_OFF', ride_id, NULL)) OVER (PARTITION BY driver_id, session_id, session_status) AS session_dropoff_count,
        SUM(revenue) OVER (PARTITION BY driver_id, session_id, session_status) AS session_revenue_sum,
        SUM(IF(activity_status = 'DISPATCHING', activity_duration_minute, 0)) OVER (PARTITION BY driver_id, session_id, session_status) AS session_dispatching_minute_sum,
        SUM(IF(activity_status = 'RIDING', activity_duration_minute, 0)) OVER (PARTITION BY driver_id, session_id, session_status) AS session_riding_minute_sum,
    
      FROM
        session_marked
    ) AS sm

    LEFT JOIN vehicle_type AS vt
      ON sm.vehicle_id = vt.vehicle_id
),

driver_working_session_info AS (
  SELECT 
    DISTINCT
    session_start_date_kr,
    driver_id,
    is_individual_business_driver,
    IFNULL(vehicle_id, IF(session_status = 'OFF', NULL, LAST_VALUE(vehicle_id IGNORE NULLS) OVER (PARTITION BY driver_id, session_id ORDER BY seq_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING))) AS vehicle_id,
    IFNULL(vehicle_region_type, LAST_VALUE(vehicle_region_type IGNORE NULLS) OVER (PARTITION BY driver_id ORDER BY session_start_at_kr ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)) AS vehicle_region_type,
    IFNULL(service_region, LAST_VALUE(service_region IGNORE NULLS) OVER (PARTITION BY driver_id ORDER BY session_start_at_kr ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)) AS service_region,
    IFNULL(vehicle_ride_type, LAST_VALUE(vehicle_ride_type IGNORE NULLS) OVER (PARTITION BY driver_id ORDER BY session_start_at_kr ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)) AS vehicle_ride_type,
    session_id,
    session_status,
    session_start_at_kr,
    session_end_at_kr,
    DATETIME_DIFF(session_end_at_kr, session_start_at_kr, SECOND) / 3600 AS session_duration_hour,
    session_ride_count,
    session_dropoff_count,
    session_revenue_sum,
    session_dispatching_minute_sum / 60 AS session_dispatching_hour,
    session_riding_minute_sum / 60 AS session_riding_hour,
    SAFE_DIVIDE(session_dropoff_count, DATETIME_DIFF(session_end_at_kr, session_start_at_kr, SECOND) / 3600) AS dropoff_per_working_hour,
    SAFE_DIVIDE(session_revenue_sum, DATETIME_DIFF(session_end_at_kr, session_start_at_kr, SECOND) / 3600) AS revenue_per_working_hour

  FROM
    final
),

active_driver_cnt AS (
  SELECT
    FORMAT_DATE('%Y-%m', session_start_date_kr) AS year_month,
    COUNT(DISTINCT vehicle_id) AS vehicle_active_cnt

  FROM
    driver_working_session_info

  WHERE
    1=1
    AND session_duration_hour >= 1
    AND session_dropoff_count > 0
    
  GROUP BY
    year_month
)


SELECT
  FORMAT_DATE('%Y-%m', date_range) AS year_month_kr,
  FORMAT_DATE('%Y-%m-01', date_range) AS month_start_date_kr,
  DATE_SUB(DATE_ADD(SAFE_CAST(FORMAT_DATE('%Y-%m-01', date_range) AS DATE), INTERVAL 1 MONTH), INTERVAL 1 DAY) AS month_end_date_kr,
  ride_base.customer_cnt AS customer_mpu_cnt,
  ride_base.avg_eta_secs AS avg_eta_secs,
  ride_base.avg_ata_secs AS avg_ata_secs,
  active_driver_cnt.vehicle_active_cnt

FROM UNNEST([date "{target_date}"]) AS date_range
  LEFT JOIN ride_base ON
    FORMAT_DATE('%Y-%m', date_range) = ride_base.year_month
  LEFT JOIN active_driver_cnt ON
    FORMAT_DATE('%Y-%m', date_range) = active_driver_cnt.year_month;