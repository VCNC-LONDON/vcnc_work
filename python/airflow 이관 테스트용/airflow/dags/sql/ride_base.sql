WITH _tip AS (
  SELECT
    ride_id,
    SUM(paid_amount) AS tip_amount
  FROM `kr-co-vcnc-tada.tada.payment` AS p
  WHERE extra_type = "TIP" 
  GROUP BY ride_id
  
), _credit_deposit AS (
  SELECT 
    ride_id,
    SUM(amount) credit_deposit_amount
  FROM `kr-co-vcnc-tada.tada.user_credit_deposit` cd
  WHERE ride_id IS NOT NULL
    AND cancelled_at IS NULL
  GROUP BY ride_id
  
), _credit_withdrawal AS (
  SELECT 
    ride_id,
    SUM(amount) credit_withdrawal_amount
  FROM `kr-co-vcnc-tada.tada.user_credit_withdrawal`
  WHERE ride_id IS NOT NULL
    AND cancelled_at IS NULL
  GROUP BY ride_id
  
), _demand_base AS (
  WITH __distinct_ride_category AS (
    SELECT DISTINCT *
    FROM `kr-co-vcnc-tada.tada_ext.ride_category`
  )
  SELECT
    LEAD(r.created_at) OVER (PARTITION BY r.rider_id ORDER BY r.created_at) AS lead_created_at,
    LAG(r.created_at) OVER (PARTITION BY r.rider_id ORDER BY r.created_at) AS lag_created_at,
    CASE WHEN REGEXP_CONTAINS(origin_address, "성남") THEN "성남"
         ELSE SUBSTR(SPLIT(origin_address, " ")[OFFSET(0)], 1, 2) END AS origin_region,
    CASE WHEN REGEXP_CONTAINS(destination_address, "성남") THEN "성남"
         ELSE SUBSTR(SPLIT(destination_address, " ")[OFFSET(0)], 1, 2) END AS destination_region,
    SUM(by_driver.rating) OVER (PARTITION BY r.rider_id ORDER BY r.created_at ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS sum_rating_by_driver_last_30,
    rc.call_view_type,
    CASE WHEN rc.call_view_type LIKE '%LITE%' THEN 'LITE'
         WHEN rc.call_view_type LIKE '%PREMIUM%' THEN 'PREMIUM'
         WHEN rc.call_view_type LIKE '%NXT%' THEN 'NXT'
         WHEN rc.call_view_type LIKE '%NEAR_TAXI%' THEN 'NEAR_TAXI'
         WHEN JSON_EXTRACT(data, '$.typeGroup') LIKE '%NEAR_TAXI%' THEN 'NEAR_TAXI'
         WHEN rc.call_view_type LIKE '%DAERI%' THEN 'DAERI'
         ELSE r.type END AS call_service_type,
    IFNULL(rc.determined_type, r.type) AS determined_vehicle_type,
    r.*,
    t.tip_amount,
    d.type AS driver_type,
    d.is_individual_business AS is_individual_business_driver,
    d.agency_id AS driver_agency_id,
    d.agency_name AS driver_agency_name,
    by_user.rating AS rating_by_user,
    by_user.reason AS rating_by_user_reason,
    by_user.created_at AS rating_by_user_created_at,
    by_user.ignored_at AS rating_by_user_ignored_at,
    ARRAY_TO_STRING(by_user.tags, ',') AS rating_by_user_tags,
    by_driver.rating AS rating_by_driver,
    by_driver.reason AS rating_by_driver_reason,
    by_driver.created_at AS rating_by_driver_created_at,
    v.taxi_region_type AS vehicle_region_type,
    v.license_plate AS vehicle_license_plate,
    pm.card_type,
    rcp.system_coupon_tracking_identifier,
    rcp.name coupon_name,
    cd.credit_deposit_amount,
    cw.credit_withdrawal_amount,
    NOT SAFE_CAST(JSON_VALUE(re.data, '$.usingMechanicalMeter') AS BOOLEAN) AS is_app_meter,
    IF(rr.ride_id IS NOT NULL, True, False) AS is_reservation
  FROM `kr-co-vcnc-tada.tada.ride` AS r
  LEFT JOIN __distinct_ride_category AS rc
  ON r.id = rc.ride_id
  LEFT JOIN tada.ride_ex AS re
  ON r.id = re.ride_id
  LEFT JOIN tada_ext.ride_review_by_user_ext AS by_user
  ON r.id = by_user.ride_id
  LEFT JOIN tada.ride_review_by_driver AS by_driver
  ON r.id = by_driver.ride_id
  LEFT JOIN tada_ext.driver_ext AS d
  ON r.driver_id = d.id
  LEFT JOIN _tip AS t
  ON r.id = t.ride_id
  LEFT JOIN tada.vehicle AS v
  ON r.vehicle_id = v.id
  LEFT JOIN tada.payment_method AS pm
  ON r.payment_method_id = pm.id
  LEFT JOIN tada.ride_coupon AS rcp
  ON r.coupon_id = rcp.id
  LEFT JOIN _credit_deposit AS cd
  ON r.id = cd.ride_id
  LEFT JOIN _credit_withdrawal AS cw
  ON r.id = cw.ride_id
  LEFT JOIN (SELECT ride_id FROM `kr-co-vcnc-tada.tada.ride_reservation`) AS rr
  ON r.id = rr.ride_id
  WHERE r.type IN ('LITE', 'PREMIUM', 'NXT')
  
), demand_base AS (
  SELECT
    CASE WHEN TIMESTAMP_DIFF(created_at, lag_created_at, minute) >= 5 OR lag_created_at IS NULL THEN TRUE
         ELSE FALSE END AS is_valid_5_first,
    CASE WHEN TIMESTAMP_DIFF(created_at, lag_created_at, minute) >= 20 OR lag_created_at IS NULL THEN TRUE
         ELSE FALSE END AS is_valid_20_first,
    CASE WHEN status = 'DROPPED_OFF' OR TIMESTAMP_DIFF(lead_created_at, created_at, minute) >= 5 OR lead_created_at IS NULL THEN TRUE
         ELSE FALSE END AS is_valid_5_last,
    CASE WHEN status = 'DROPPED_OFF' OR TIMESTAMP_DIFF(lead_created_at, created_at, minute) >= 20 OR lead_created_at IS NULL THEN TRUE
         ELSE FALSE END AS is_valid_20_last,
    TIMESTAMP_DIFF(lead_created_at, created_at, minute) AS minute_diff,
    TIMESTAMP_DIFF(lead_created_at, created_at, second) AS second_diff,
    *
  FROM _demand_base
  
), _session_partitioned AS (
  SELECT
    IF(is_valid_5_first, RANK() OVER (PARTITION BY rider_id, is_valid_5_first ORDER BY created_at), NULL) AS demand_session_id,
    *
  FROM demand_base
  
), session_partitioned AS (
  SELECT
    IFNULL(demand_session_id, MAX(demand_session_id) OVER (PARTITION BY rider_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS demand_session_id,
    * EXCEPT(demand_session_id)
  FROM _session_partitioned 
  
), session_typed AS (
  SELECT
    (session_call_count = session_lite_count
     OR session_call_count = session_plus_count
     OR session_call_count = session_next_count
     OR session_call_count = session_near_taxi_count) AS is_pure_session,
    CASE WHEN (session_call_count = session_lite_count OR session_call_count = session_plus_count OR session_call_count = session_next_count OR session_call_count = session_near_taxi_count) THEN call_service_type
         ELSE "MIXED" END AS session_type,
    *
  FROM (
    SELECT
      COUNT(demand_session_id) OVER (PARTITION BY rider_id, demand_session_id) AS session_call_count,
      COUNTIF(call_service_type = "LITE") OVER (PARTITION BY rider_id, demand_session_id) AS session_lite_count,
      COUNTIF(call_service_type = "PREMIUM") OVER (PARTITION BY rider_id, demand_session_id) AS session_plus_count,
      COUNTIF(call_service_type = "NXT") OVER (PARTITION BY rider_id, demand_session_id) AS session_next_count,
      COUNTIF(call_service_type = "NEAR_TAXI") OVER (PARTITION BY rider_id, demand_session_id) AS session_near_taxi_count,
      MIN(created_at) OVER (PARTITION BY rider_id, demand_session_id) AS session_start_at,
      MAX(created_at) OVER (PARTITION BY rider_id, demand_session_id) AS session_end_at,
      FIRST_VALUE(call_service_type) OVER (PARTITION BY rider_id, demand_session_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_call_service_type,
      LAST_VALUE(call_service_type) OVER (PARTITION BY rider_id, demand_session_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_call_service_type,
      LAST_VALUE(status) OVER (PARTITION BY rider_id, demand_session_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS session_final_status,
      *
    FROM session_partitioned
  )
  
), final_passport AS (
SELECT 
  DISTINCT st.*,
    MAX(IF(IFNULL(st.dropped_off_at, st.created_at) BETWEEN um.created_at AND IFNULL(um.inactivated_at, um.expires_at), TRUE, FALSE)) OVER (PARTITION BY st.id) is_passport_ride,
    MAX(IF(IFNULL(st.dropped_off_at, st.created_at) BETWEEN um.created_at AND IFNULL(um.inactivated_at, um.expires_at), um.source, NULL)) OVER (PARTITION BY st.id) passport_source,
    MAX(IF(IFNULL(st.dropped_off_at, st.created_at) BETWEEN um.created_at AND IFNULL(um.inactivated_at, um.expires_at), um.product_identifier, NULL)) OVER (PARTITION BY st.id) passport_product_identifier
  FROM session_typed st
  LEFT JOIN `kr-co-vcnc-tada.tada.user_membership` um
  ON st.rider_id = um.user_id
  
), final AS (
  SELECT
    -- 호출의 created_at 기준 날짜 정보
    date_kr,
    EXTRACT(ISOYEAR FROM date_kr) isoyear,
    EXTRACT(ISOWEEK FROM date_kr) isoweek,
    EXTRACT(MONTH FROM date_kr) month,
    -- 호출 정보
    id AS ride_id,
    call_view_type,
    call_service_type,
    determined_vehicle_type,
    type,
    status,
    is_passport_ride,
    -- 시간 정보
    DATETIME(created_at, 'Asia/Seoul') AS created_at_kr,
    DATETIME(accepted_at, 'Asia/Seoul') AS accepted_at_kr,
    DATETIME(arrived_at, 'Asia/Seoul') AS arrived_at_kr,
    DATETIME(picked_up_at, 'Asia/Seoul') AS picked_up_at_kr,
    DATETIME(arrived_at_destination_at, 'Asia/Seoul') AS arrived_at_destination_at_kr,
    DATETIME(dropped_off_at, 'Asia/Seoul') AS dropped_off_at_kr,
    DATETIME(cancelled_at, 'Asia/Seoul') AS cancelled_at_kr,
    -- 지역 정보
    origin_region,
    origin_lat,
    origin_lng,
    origin_name,
    origin_address,
    `tada_udf.geo_to_h3`(origin_lng, origin_lat, 7) AS origin_h3_l7,
    `tada_udf.geo_to_h3`(origin_lng, origin_lat, 9) AS origin_h3_l9,
    pickup_lat,
    pickup_lng,
    pickup_name,
    pickup_address,
    destination_region,
    destination_lat,
    destination_lng,
    destination_name,
    destination_address,
    `tada_udf.geo_to_h3`(destination_lng, destination_lat, 7) AS destination_h3_l7,
    `tada_udf.geo_to_h3`(destination_lng, destination_lat, 9) AS destination_h3_l9,
    dropoff_lat,
    dropoff_lng,
    dropoff_name,
    dropoff_address,
    -- 취소 정보
    cancellation_cause,
    cancellation_reason,
    cancellation_reason_type driver_cancellation_reason,
    -- 여정 이동정보
    distance_meters,
    adjusted_distance_meters,
    estimation_distance_meters,
    estimation_duration_seconds,
    -- 서징 및 가격 정보
    surge_percentage,
    price_elasticity_testing_percentage,
    estimation_min_cost,
    estimation_max_cost,
    estimation_original_min_cost,
    estimation_original_max_cost,
    -- 결제 정보
    coupon_id,
    coupon_name,
    system_coupon_tracking_identifier,
    payment_method_id,
    payment_profile_id,
    card_type,
    receipt_basic_fee,
    receipt_distance_based_fee,
    receipt_time_based_fee,
    receipt_drive_fee,
    receipt_discount_amount,
    receipt_employee_discount_amount,
    receipt_tollgate_fee,
    receipt_total,
    receipt_refund_amount,
    receipt_extra,
    receipt_cancellation_fee,
    tip_amount AS receipt_tip_amount,
    credit_deposit_amount,
    credit_withdrawal_amount,
    -- 법인 정보
    biz_reason,
    biz_reason_text,
    -- ETA 정보
    origin_eta_multiplier,
    DATETIME(origin_eta, 'Asia/Seoul') origin_eta_kr,
    DATETIME(destination_eta, 'Asia/Seoul') destination_eta_kr,
    -- 세션 정보
    is_pure_session,
    session_type,
    session_final_status,
    session_call_count,
    session_lite_count,
    session_plus_count,
    session_near_taxi_count,
    DATETIME(session_start_at, 'Asia/Seoul') AS session_start_at_kr,
    DATETIME(session_end_at, 'Asia/Seoul') AS session_end_at_kr,
    first_call_service_type,
    last_call_service_type,
    demand_session_id,
    is_valid_5_first,
    is_valid_20_first,
    is_valid_5_last,
    is_valid_20_last,
    -- 유저 정보
    rider_id,
    sum_rating_by_driver_last_30,
    -- 드라이버 정보
    driver_id,
    driver_type,
    is_individual_business_driver,
    driver_agency_id,
    driver_agency_name,
    -- 차량 정보
    vehicle_id,
    vehicle_license_plate,
    vehicle_region_type,
    -- 유저 -> 드라이버 평가 정보
    rating_by_user,
    rating_by_user_reason,
    DATETIME(rating_by_user_created_at, 'Asia/Seoul') AS rating_by_user_created_at_kr,
    DATETIME(rating_by_user_ignored_at, 'Asia/Seoul') AS rating_by_user_ignored_at_kr,
    rating_by_user_tags,
    -- 드라이버 -> 유저 평가 정보
    rating_by_driver,
    rating_by_driver_reason,
    DATETIME(rating_by_driver_created_at, 'Asia/Seoul') AS rating_by_driver_created_at_kr,
    -- 패스포트 정보
    passport_source,
    passport_product_identifier,
    -- 앱미터기 적용 유무
    is_app_meter,
    -- 호출예약 콜 유무
    is_reservation
  FROM final_passport
)

SELECT *
FROM final