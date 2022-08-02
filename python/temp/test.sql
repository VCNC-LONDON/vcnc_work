WITH store_aggregated AS (
  SELECT
    date_kr,
    a.origin_region,
    replace(a.type, 'PREMIUM', 'PLUS') AS driver_type,
    --수요
    count(is_valid_5_last) as call_count_total,
    countif(is_valid_5_last is TRUE) AS call_count_valid_session,
    --운행
    countif(a.status = 'DROPPED_OFF') AS dropoff_count,
    --서지
    avg(
      case
        when a.status = 'DROPPED_OFF' then ifnull(a.surge_percentage, 100) end) AS surge_rate_avg,
    --요금
    sum(
      ifnull(
        credit_withdrawal_amount, 0)) / 1.1 AS credit_withdrawal_amount_without_vat

  FROM
    UNNEST(generate_date_array(date '2021-11-25', current_date('Asia/Seoul'))) AS date_kr
        LEFT JOIN tada_store.ride_base AS a
            ON date_kr = a.date_kr
    
  WHERE
    1=1
    AND a.date_kr between date_sub("{target_date}", INTERVAL 1 DAY) and "{target_date}"

  GROUP BY
    date_kr,
    a.origin_region,
    driver_type
),

ride_estimation_base AS (
  SELECT
    * EXCEPT(origin_region),
    #1ms 차이로 타입이 다른 같은 뷰가 갈라지는 경우가 있어서 처리
    CASE WHEN time_ms = LAG(time_ms) OVER (PARTITION BY rider_id ORDER BY time_ms, type_order) + 1 THEN time_ms - 1
      ELSE time_ms END AS revised_time_ms,
    IFNULL(origin_region, LAG(origin_region) OVER (PARTITION BY rider_id ORDER BY time_ms)) AS origin_region
   
  FROM (
    SELECT
      * EXCEPT(estimation_ride_type),
      CASE WHEN estimation_ride_type = 'LITE' THEN 1
        WHEN estimation_ride_type = 'PREMIUM' THEN 2
        WHEN estimation_ride_type = 'NXT' THEN 3
        ELSE 4 END type_order,
      IFNULL(alternative_ride_types_list[SAFE_OFFSET(0)].element, estimation_ride_type) AS estimation_ride_type,
      CASE WHEN REGEXP_CONTAINS(origin_address, "성남시") THEN "성남"
        ELSE SUBSTR(SPLIT(origin_address, " ")[OFFSET(0)], 1, 2) END AS origin_region,
    
    FROM tada_ext.ride_estimation_ext
    
    WHERE
      1=1
      and date_kr between date_sub("{target_date}", INTERVAL 1 DAY) and "{target_date}"
  )
  
  WHERE
    1=1
    and estimation_ride_type IN ('LITE', 'PREMIUM', 'NXT', 'NEAR_TAXI')
   
),

ride_estimation AS (
  SELECT 
    *,
    CONCAT(rider_id, "-", revised_time_ms) AS rider_id_time_ms,
    ARRAY_AGG(estimation_uuid) OVER (PARTITION BY CONCAT(rider_id, "-", revised_time_ms) ORDER BY type_order ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS estimation_uuid_array, 
    ARRAY_AGG(estimation_ride_type) OVER (PARTITION BY CONCAT(rider_id, "-", revised_time_ms) ORDER BY type_order ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ride_type_array,
    ARRAY_AGG(min_cost) OVER (PARTITION BY CONCAT(rider_id, "-", revised_time_ms) ORDER BY type_order ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS min_cost_array,
    ARRAY_AGG(max_cost) OVER (PARTITION BY CONCAT(rider_id, "-", revised_time_ms) ORDER BY type_order ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_cost_array
  
  FROM ride_estimation_base
),

ride_estimation_agg AS (
  SELECT
    DISTINCT
    date_kr,
    log_type,
    revised_time_ms time_ms,
    DATETIME(TIMESTAMP_MILLIS(revised_time_ms), "Asia/Seoul") AS estimate_time_kr,
    rider_id,
    origin_region,
    origin_address,
    destination_address,
    rider_id_time_ms,
    distance_meters,
    duration_seconds,
    ARRAY_TO_STRING(estimation_uuid_array, ",") AS combined_estimation_uuid,
    ARRAY_TO_STRING(ride_type_array, ",") AS ride_type_array,
    CASE WHEN ride_type_array[SAFE_OFFSET(0)] = "LITE" THEN estimation_uuid_array[SAFE_OFFSET(0)]
      WHEN ride_type_array[SAFE_OFFSET(1)] = "LITE" THEN estimation_uuid_array[SAFE_OFFSET(1)]
      ELSE NULL END AS lite_estimation_uuid,
    CASE WHEN ride_type_array[SAFE_OFFSET(0)] = "PREMIUM" THEN estimation_uuid_array[SAFE_OFFSET(0)]
      WHEN ride_type_array[SAFE_OFFSET(1)] = "PREMIUM" THEN estimation_uuid_array[SAFE_OFFSET(1)]
      ELSE NULL END AS plus_estimation_uuid,
    CASE WHEN ride_type_array[SAFE_OFFSET(0)] = "NXT" THEN estimation_uuid_array[SAFE_OFFSET(0)]
      WHEN ride_type_array[SAFE_OFFSET(1)] = "NXT" THEN estimation_uuid_array[SAFE_OFFSET(1)]
      ELSE NULL END AS next_estimation_uuid,
    lite_unavailable_by_no_nearby_vehicle,
    CASE WHEN ride_type_array[SAFE_OFFSET(0)] = "LITE" THEN min_cost_array[SAFE_OFFSET(0)]
      WHEN ride_type_array[SAFE_OFFSET(1)] = "LITE" THEN min_cost_array[SAFE_OFFSET(1)]
      ELSE NULL END AS lite_min_cost,
    CASE WHEN ride_type_array[SAFE_OFFSET(0)] = "LITE" THEN max_cost_array[SAFE_OFFSET(0)]
      WHEN ride_type_array[SAFE_OFFSET(1)] = "LITE" THEN max_cost_array[SAFE_OFFSET(1)]
      ELSE NULL END AS lite_max_cost,
    CASE WHEN ride_type_array[SAFE_OFFSET(0)] = "PREMIUM" THEN min_cost_array[SAFE_OFFSET(0)]
      WHEN ride_type_array[SAFE_OFFSET(1)] = "PREMIUM" THEN min_cost_array[SAFE_OFFSET(1)]
      ELSE NULL END AS plus_min_cost,
    CASE WHEN ride_type_array[SAFE_OFFSET(0)] = "PREMIUM" THEN max_cost_array[SAFE_OFFSET(0)]
      WHEN ride_type_array[SAFE_OFFSET(1)] = "PREMIUM" THEN max_cost_array[SAFE_OFFSET(1)]
      ELSE NULL END AS plus_max_cost,
    CASE WHEN ride_type_array[SAFE_OFFSET(0)] = "NXT" THEN min_cost_array[SAFE_OFFSET(0)]
      WHEN ride_type_array[SAFE_OFFSET(1)] = "NXT" THEN min_cost_array[SAFE_OFFSET(1)]
      ELSE NULL END AS next_min_cost,
    CASE WHEN ride_type_array[SAFE_OFFSET(0)] = "NXT" THEN max_cost_array[SAFE_OFFSET(0)]
      WHEN ride_type_array[SAFE_OFFSET(1)] = "NXT" THEN max_cost_array[SAFE_OFFSET(1)]
      ELSE NULL END AS next_max_cost,
  
  FROM ride_estimation
),

ride_request AS (
  SELECT 
    req.date_kr,
    req.log_type,
    req.estimation_uuid,
    req.time_ms,
    req.time_kr,
    req.rider_id,
    req.ride_id,
    req.ride_type,
    req.origin_address,
    req.destination_address,
    req.origin_source,
    req.destination_source,
    CASE WHEN REGEXP_CONTAINS(req.origin_address, "성남시") THEN "성남"
      ELSE SUBSTR(SPLIT(req.origin_address, " ")[OFFSET(0)], 1, 2) END AS origin_region,    
    CASE WHEN req.ride_type = "LITE" AND req.estimation_uuid IS NOT NULL THEN req.estimation_uuid
      WHEN JSON_EXTRACT_SCALAR(re.data, "$.nearRideEstimationSnapshot.rideEstimationSnapshots[0].rideType") = "LITE" THEN JSON_EXTRACT_SCALAR(re.data, "$.nearRideEstimationSnapshot.rideEstimationSnapshots[0].uuid")
      WHEN JSON_EXTRACT_SCALAR(re.data, "$.nearRideEstimationSnapshot.rideEstimationSnapshots[1].rideType") = "LITE" THEN JSON_EXTRACT_SCALAR(re.data, "$.nearRideEstimationSnapshot.rideEstimationSnapshots[1].uuid")
      ELSE NULL END AS lite_estimation_uuid,
    CASE WHEN req.ride_type = "PREMIUM" AND req.estimation_uuid IS NOT NULL THEN req.estimation_uuid
      WHEN JSON_EXTRACT_SCALAR(re.data, "$.nearRideEstimationSnapshot.rideEstimationSnapshots[0].rideType") = "PREMIUM" THEN JSON_EXTRACT_SCALAR(re.data, "$.nearRideEstimationSnapshot.rideEstimationSnapshots[0].uuid")
      WHEN JSON_EXTRACT_SCALAR(re.data, "$.nearRideEstimationSnapshot.rideEstimationSnapshots[1].rideType") = "PREMIUM" THEN JSON_EXTRACT_SCALAR(re.data, "$.nearRideEstimationSnapshot.rideEstimationSnapshots[1].uuid")
      ELSE NULL END AS plus_estimation_uuid,   
    CASE WHEN req.ride_type = "NXT" AND req.estimation_uuid IS NOT NULL THEN req.estimation_uuid
      WHEN JSON_EXTRACT_SCALAR(re.data, "$.nearRideEstimationSnapshot.rideEstimationSnapshots[0].rideType") = "NXT" THEN JSON_EXTRACT_SCALAR(re.data, "$.nearRideEstimationSnapshot.rideEstimationSnapshots[0].uuid")
      WHEN JSON_EXTRACT_SCALAR(re.data, "$.nearRideEstimationSnapshot.rideEstimationSnapshots[1].rideType") = "NXT" THEN JSON_EXTRACT_SCALAR(re.data, "$.nearRideEstimationSnapshot.rideEstimationSnapshots[1].uuid")
      ELSE NULL END AS next_estimation_uuid,
         
  FROM tada_ext.ride_request_ext AS req
    LEFT JOIN tada.ride_ex AS re
      ON req.ride_id = re.ride_id
  
  WHERE
    1=1
    AND req.date_kr between date_sub("{target_date}", INTERVAL 1 DAY) and "{target_date}"
    AND ride_type IN ('LITE', 'PREMIUM', 'NXT', 'NEAR_TAXI')  
),

ride_request_uuid_filled AS (
  SELECT 
    * EXCEPT(lite_estimation_uuid, plus_estimation_uuid, next_estimation_uuid),
    IF(lite_estimation_uuid IS NULL AND plus_estimation_uuid IS NULL AND next_estimation_uuid IS NULL,
    LAST_VALUE(lite_estimation_uuid IGNORE NULLS) OVER (PARTITION BY rider_id ORDER BY time_ms ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    lite_estimation_uuid) AS lite_estimation_uuid,
    IF(lite_estimation_uuid IS NULL AND plus_estimation_uuid IS NULL AND next_estimation_uuid IS NULL,
    LAST_VALUE(plus_estimation_uuid IGNORE NULLS) OVER (PARTITION BY rider_id ORDER BY time_ms ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    plus_estimation_uuid) AS plus_estimation_uuid,
    IF(lite_estimation_uuid IS NULL AND plus_estimation_uuid IS NULL AND next_estimation_uuid IS NULL,
    LAST_VALUE(next_estimation_uuid IGNORE NULLS) OVER (PARTITION BY rider_id ORDER BY time_ms ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    next_estimation_uuid) AS next_estimation_uuid
       
  FROM ride_request
),

ride_request_final AS (
  SELECT 
    rr.*,
    d.is_valid_5_last,
    d.session_type,
    d.status,
    IF(d.accepted_at_kr IS NOT NULL, d.type, "UNMATCHED") AS matched_vehicle_type

  FROM ride_request_uuid_filled AS rr
    LEFT JOIN tada_store.ride_base AS d
      ON rr.ride_id = d.ride_id
),

ride_estimation_request_joined AS (
  SELECT 
    DISTINCT
    re.* EXCEPT(combined_estimation_uuid, ride_type_array),
    IFNULL(IFNULL(rr1.ride_id, rr2.ride_id), rr3.ride_id) AS ride_id,
    IFNULL(IFNULL(rr1.log_type, rr2.log_type), rr3.log_type) AS request_log_type,
    IFNULL(IFNULL(rr1.time_ms, rr2.time_ms), rr3.time_ms) AS request_time_ms,
    IFNULL(IFNULL(rr1.time_kr, rr2.time_kr), rr3.time_kr) AS request_time_kr,
    IFNULL(IFNULL(rr1.rider_id, rr2.rider_id), rr3.rider_id) AS request_rider_id,
    IFNULL(IFNULL(rr1.ride_type, rr2.ride_type), rr3.ride_type) AS request_ride_type,
    IFNULL(IFNULL(rr1.origin_address, rr2.origin_address), rr3.origin_address) AS request_origin_address,
    IFNULL(IFNULL(rr1.destination_address, rr2.destination_address), rr3.destination_address) AS request_destination_address,
    IFNULL(IFNULL(rr1.origin_source, rr2.origin_source), rr3.origin_source) AS request_origin_source,
    IFNULL(IFNULL(rr1.destination_source, rr2.destination_source), rr3.destination_source) AS request_destination_source,
    IFNULL(IFNULL(rr1.origin_region, rr2.origin_region), rr3.origin_region) AS request_origin_region,
    IFNULL(IFNULL(rr1.lite_estimation_uuid, rr2.lite_estimation_uuid), rr3.lite_estimation_uuid) AS request_lite_estimation_uuid,
    IFNULL(IFNULL(rr1.plus_estimation_uuid, rr2.plus_estimation_uuid), rr3.plus_estimation_uuid) AS request_plus_estimation_uuid,
    IFNULL(IFNULL(rr1.next_estimation_uuid, rr2.next_estimation_uuid), rr3.next_estimation_uuid) AS request_next_estimation_uuid,
    IFNULL(IFNULL(rr1.is_valid_5_last, rr2.is_valid_5_last), rr3.is_valid_5_last) AS is_valid_5_last,
    IFNULL(IFNULL(rr1.session_type, rr2.session_type), rr3.session_type) AS session_type,
    IFNULL(IFNULL(rr1.status, rr2.status), rr3.status) AS ride_status,
    IFNULL(IFNULL(rr1.matched_vehicle_type, rr2.matched_vehicle_type), rr3.matched_vehicle_type) AS matched_vehicle_type
    
  FROM
    ride_estimation_agg AS re
      LEFT JOIN ride_request_final AS rr1
        ON re.lite_estimation_uuid = rr1.lite_estimation_uuid
      LEFT JOIN ride_request_final AS rr2
        ON re.plus_estimation_uuid = rr2.plus_estimation_uuid
      LEFT JOIN ride_request_final AS rr3
        ON re.next_estimation_uuid = rr3.next_estimation_uuid
),

valid_view AS (
  SELECT
    *,
    DATETIME_DIFF(lead_estimate_time_kr, estimate_time_kr, SECOND) / 60 AS lead_estimate_time_diff_minute,
    CASE WHEN (
      is_valid_5_last OR lead_estimate_time_kr IS NULL
        OR DATETIME_DIFF(lead_estimate_time_kr, estimate_time_kr, SECOND) / 60 >= 5) THEN TRUE
      ELSE FALSE END is_valid_estimate_view
  
  FROM (
    SELECT 
      *,
      LEAD(estimate_time_kr) OVER (PARTITION BY rider_id ORDER BY estimate_time_kr, request_time_kr) lead_estimate_time_kr
    
    FROM ride_estimation_request_joined
  )
),

valid_estimation_view AS (
  SELECT
    date_kr,
    origin_region,
    count(
      distinct if(
        is_valid_estimate_view, rider_id_time_ms, NULL)) AS valid_estimation_view_count

  FROM
    valid_view

  WHERE
    1=1
    AND date_kr between date_sub("{target_date}", INTERVAL 1 DAY) and "{target_date}"

  GROUP BY
    date_kr,
    origin_region
),

driver_dropoff_agg AS (
  SELECT
    date_kr,
    origin_region, 
    type, 
    driver_id, 
    vehicle_id, 
    countif(status = "DROPPED_OFF") AS dropoff_count
    
  FROM
    tada_store.ride_base

  WHERE
    1=1
    AND date_kr between date_sub("{target_date}", INTERVAL 1 DAY) and "{target_date}"
    
  GROUP BY
    date_kr,
    origin_region,
    type,
    driver_id,
    vehicle_id
),

driver_activity_base AS (
  SELECT 
    dab.date_kr, 
    dab.driver_id,
    dab.is_individual_business_driver,
    replace(dab.vehicle_ride_type, 'PREMIUM', 'PLUS') AS vehicle_ride_type,
    dab.service_region,
    sum(
      case
        when dab.activity_status IN ('IDLE', 'DISPATCHING', 'RIDING') THEN dab.day_activity_duration_second
        else 0 end) / 3600 AS working_hour,
    sum(
      case
        when dab.activity_status IN ('IDLE') THEN dab.day_activity_duration_second
        else 0 end) / 3600 AS idle_hour,
    sum(
      case
        when dab.activity_status IN ('DISPATCHING') THEN dab.day_activity_duration_second
        else 0 end) / 3600 AS dispatching_hour,
    sum(
      case
        when dab.activity_status IN ('RIDING') THEN dab.day_activity_duration_second
        else 0 end) / 3600 AS riding_hour,
    sum(
      case
        when dab.activity_status IN ('RIDING') AND dab.ride_status IN ('PICKED_UP') THEN dab.day_activity_duration_second
        else 0 end) / 3600 AS picked_up_hour,
    ifnull(dd.dropoff_count, 0) AS dropoff_count
    
  FROM
    tada_store.driver_activity_base AS dab
        LEFT JOIN driver_dropoff_agg AS dd
            ON dab.date_kr = dd.date_kr
                AND dab.service_region = dd.origin_region
                AND dab.vehicle_ride_type = dd.type
                AND dab.driver_id = dd.driver_id
    
  WHERE
    1=1
    AND dab.date_kr between date_sub("{target_date}", INTERVAL 1 DAY) and "{target_date}"
    AND dab.activity_status IN ('IDLE', 'DISPATCHING', 'RIDING')
    
  GROUP BY
    date_kr,
    driver_id,
    is_individual_business_driver,
    vehicle_ride_type,
    service_region,
    dropoff_count
),

driver_activity_aggregated AS (
  SELECT
    date_kr,
    service_region AS region,
    vehicle_ride_type AS driver_type,
    sum(picked_up_hour) AS picked_up_hour,
    sum(riding_hour) + sum(dispatching_hour) AS riding_dispatching_hour
    
  FROM
    driver_activity_base

  GROUP BY
    date_kr,
    region,
    driver_type
),

net_revenue AS (
  SELECT 
    date_kr,
    replace(type, 'PREMIUM', 'PLUS') AS type,
    case
      when region in ('서울_경기', '부천', '인천') THEN '서울' else region end AS region,
    sum(revenue) AS revenue_no_credit,
    sum(revenue_net) AS net_revenue,
    sum(revenue) / 1.1 AS revenue_no_credit_without_vat,
    sum(revenue_net) / 1.1 AS net_revenue_without_vat

  FROM
    tada_metric.raw_ride_receipt
    
  WHERE
    1=1
    AND date_kr between date_sub("{target_date}", INTERVAL 1 DAY) and "{target_date}"
    AND type IN ('LITE', 'PREMIUM', 'NXT')

  GROUP BY
    date_kr,
    type,
    region
),

finalised AS (
  SELECT
    a.date_kr,
    a.origin_region AS region,
    a.driver_type,
    b.valid_estimation_view_count AS valid_estimation_view_count_duplicated,
    a.* EXCEPT(date_kr, origin_region, driver_type, credit_withdrawal_amount_without_vat),
    f.revenue_no_credit_without_vat + a.credit_withdrawal_amount_without_vat AS revenue_without_vat,
    f.net_revenue_without_vat,
    1 - (net_revenue_without_vat / (f.revenue_no_credit_without_vat + a.credit_withdrawal_amount_without_vat)) AS discount_rate,
    c.picked_up_hour,
    c.riding_dispatching_hour,
    count(distinct e.driver_id) AS driver_active_count

  FROM
    store_aggregated AS a
      LEFT JOIN valid_estimation_view AS b
        ON a.date_kr = b.date_kr
          AND a.origin_region = b.origin_region
      LEFT JOIN driver_activity_aggregated AS c
        ON a.date_kr = c.date_kr
          AND a.origin_region = c.region
          AND a.driver_type = c.driver_type
      LEFT JOIN driver_activity_base AS d
        ON a.date_kr = d.date_kr
          AND a.origin_region = d.service_region
          AND a.driver_type = d.vehicle_ride_type
      LEFT JOIN tada_store.driver_working_session_info AS e
        ON a.date_kr = e.session_start_date_kr
          AND a.origin_region = e.service_region
          AND a.driver_type = replace(e.vehicle_ride_type, 'PREMIUM', 'PLUS')
      LEFT JOIN net_revenue AS f
        ON a.date_kr = f.date_kr
          AND a.origin_region = f.region
          AND a.driver_type = f.type

  WHERE
    1=1
    AND e.session_duration_hour >= 1
    AND session_dropoff_count > 0

  GROUP BY
    date_kr,
    region,
    driver_type,
    valid_estimation_view_count_duplicated,
    call_count_total,
    call_count_valid_session,
    dropoff_count,
    surge_rate_avg,
    revenue_without_vat,
    net_revenue_without_vat,
    discount_rate,
    picked_up_hour,
    riding_dispatching_hour
)


SELECT
    *

FROM finalised

WHERE
    1=1
    AND date_kr = "{target_date}"
    AND region in ('서울', '부산', '성남');
