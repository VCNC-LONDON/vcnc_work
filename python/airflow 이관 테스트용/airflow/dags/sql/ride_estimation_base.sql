
WITH ride_estimation_base AS (
  SELECT
    uuid AS log_uuid,
    timeMs AS time_ms,
    DATETIME_TRUNC(DATETIME(TIMESTAMP_MILLIS(timeMs), 'Asia/Seoul'), SECOND) AS time_kr,
    IFNULL(caller.userId, ride.riderId) AS rider_id,
    rideEstimations.*,
    rideEstimation.estimationUuid AS estimation_uuid
  FROM
    tada.server_log_parquet
  WHERE
    date_kr = "{target_date}"
    AND type = 'USER_LIST_RIDE_ESTIMATIONS'
  )

, ride_estimation_ext AS (
  SELECT
    log_uuid AS estimation_log_uuid,
    time_ms AS estimate_time_ms,
    time_kr AS estimate_time_kr,
    rider_id,
    l.element.rideType AS ride_type,
    l.element.couponId AS estimate_coupon_id,
    l.element.origin.address AS origin_address,
    l.element.origin.lat AS origin_lat,
    l.element.origin.lng AS origin_lng,
    CASE WHEN REGEXP_CONTAINS(l.element.origin.address, "성남시") THEN "성남" ELSE SUBSTR(SPLIT(l.element.origin.address, " ")[OFFSET(0)], 1, 2) END origin_region,
    l.element.destination.address AS destination_address,
    l.element.destination.lat AS destination_lat,
    l.element.destination.lng AS destination_lng,
    CASE WHEN REGEXP_CONTAINS(l.element.destination.address, "성남시") THEN "성남" ELSE SUBSTR(SPLIT(l.element.destination.address, " ")[OFFSET(0)], 1, 2) END destination_region,
    IFNULL(l.element.displaySurgePercentage, 100) AS surge_percentage,
    l.element.distanceMeters AS distance_meters,
    l.element.durationSeconds AS duration_seconds,
    l.element.estimationUuid AS estimation_uuid,
    l.element.minCost AS cost,
    l.element.unavailableByNoNearbyVehicleRideTypes.list[SAFE_OFFSET(0)].element AS first_no_nearby_vehicle,
    l.element.unavailableByNoNearbyVehicleRideTypes.list[SAFE_OFFSET(1)].element AS second_no_nearby_vehicle,
    l.element.unavailableByNoNearbyVehicleRideTypes.list[SAFE_OFFSET(2)].element AS third_no_nearby_vehicle,
  FROM
    ride_estimation_base, UNNEST(list) AS l
  )

, ride_estimation_agg AS (
  SELECT
    -- uuid 정보
    estimation_log_uuid,
    MAX(IF(ride_type = "NXT", estimation_uuid, NULL)) AS nxt_estimation_uuid,
    MAX(IF(ride_type = "LITE", estimation_uuid, NULL)) AS lite_estimation_uuid,
    MAX(IF(ride_type = "PREMIUM", estimation_uuid, NULL)) AS plus_estimation_uuid,
    MAX(IF(ride_type = "NEAR_TAXI", estimation_uuid, NULL)) AS near_taxi_estimation_uuid,
    -- 시간 정보
    MAX(estimate_time_kr) AS estimate_time_kr,
    DATE(MAX(estimate_time_kr)) AS date_kr,
    -- 유저 정보
    MAX(rider_id) AS rider_id,
    -- 쿠폰 정보
    MAX(estimate_coupon_id) AS estimate_coupon_id,
    -- 여정 정보
    MAX(origin_address) AS origin_address,
    MAX(origin_lat) AS origin_lat,
    MAX(origin_lng) AS origin_lng,
    MAX(origin_region) AS origin_region,
    MAX(destination_address) AS destination_address,
    MAX(destination_lat) AS destination_lat,
    MAX(destination_lng) AS destination_lng,
    MAX(destination_region) AS destination_region,
    MAX(distance_meters) AS distance_meters,
    MAX(duration_seconds) AS duration_seconds,
    -- 가격 정보
    MAX(IF(ride_type = "NXT", cost, NULL)) AS nxt_cost,
    MAX(IF(ride_type = "LITE", cost, NULL)) AS lite_cost,
    MAX(IF(ride_type = "PREMIUM", cost, NULL)) AS plus_cost,
    MAX(IF(ride_type = "NXT", surge_percentage, NULL)) AS nxt_surge_percentage,
    MAX(IF(ride_type = "LITE", surge_percentage, NULL)) AS lite_surge_percentage,
    MAX(IF(ride_type = "PREMIUM", surge_percentage, NULL)) AS plus_surge_percentage,
    -- 모차운중 정보
    CASE WHEN MAX(first_no_nearby_vehicle) = "NXT" OR MAX(second_no_nearby_vehicle) = "NXT" OR MAX(third_no_nearby_vehicle) = "NXT" THEN FALSE ELSE TRUE END AS is_nxt_nearby_vehicle,
    CASE WHEN MAX(first_no_nearby_vehicle) = "LITE" OR MAX(second_no_nearby_vehicle) = "LITE" OR MAX(third_no_nearby_vehicle) = "LITE" THEN FALSE ELSE TRUE END AS is_lite_nearby_vehicle,
    CASE WHEN MAX(first_no_nearby_vehicle) = "PREMIUM" OR MAX(second_no_nearby_vehicle) = "PREMIUM" OR MAX(third_no_nearby_vehicle) = "PREMIUM" THEN FALSE ELSE TRUE END AS is_plus_nearby_vehicle,
  FROM
    ride_estimation_ext
  GROUP BY
    estimation_log_uuid
  )

, ride_request_base AS (
  SELECT
    rqe.date_kr,
    DATETIME_TRUNC(time_kr, SECOND) AS request_time_kr,
    -- 넛지로 인한 호출일 경우 넛지 직전의 정보를 가져오기 위해 estimation_uuid를 null 으로 만들어줍니다.
    CASE WHEN original_ride_id is not null THEN null
         ELSE estimation_uuid
         END AS estimation_uuid,
    rider_id,
    rqe.ride_id,
    ride_type,
    original_ride_id,
    suggestion_type,
    coupon_id AS request_coupon_id,
  FROM
    tada_ext.ride_request_ext rqe
    LEFT JOIN tada.ride_ex re USING(ride_id)
  WHERE
    rqe.date_kr BETWEEN "{target_date}" AND DATE_ADD("{target_date}", INTERVAL 1 DAY)
  )

, ride_request_estimation_uuid_filled AS (
  SELECT
    -- 넛지로 인한 호출일 경우 처음에 넛지 직전의 estimation_uuid 로 값을 채워줍니다.
    IF(estimation_uuid IS NULL, LAST_VALUE(estimation_uuid IGNORE NULLS) OVER (PARTITION BY rider_id ORDER BY request_time_kr ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), estimation_uuid) AS estimation_uuid,
    * EXCEPT(estimation_uuid)
  FROM
    ride_request_base
  )

, estimation_request_joined_data AS (
  SELECT
    ride_estimation_agg.* EXCEPT(nxt_estimation_uuid, lite_estimation_uuid, plus_estimation_uuid, near_taxi_estimation_uuid),
    ride_request_estimation_uuid_filled.* EXCEPT(estimation_uuid, rider_id, date_kr)
  FROM
    ride_estimation_agg LEFT JOIN ride_request_estimation_uuid_filled
    ON (ride_estimation_agg.nxt_estimation_uuid = ride_request_estimation_uuid_filled.estimation_uuid
        OR ride_estimation_agg.lite_estimation_uuid = ride_request_estimation_uuid_filled.estimation_uuid
        OR ride_estimation_agg.plus_estimation_uuid = ride_request_estimation_uuid_filled.estimation_uuid
        OR ride_estimation_agg.near_taxi_estimation_uuid = ride_request_estimation_uuid_filled.estimation_uuid)
  )

, valid_view AS (
  SELECT
    * EXCEPT(lead_estimate_time_kr),
    CASE WHEN (is_valid_5_last
               OR lead_estimate_time_kr IS NULL
               OR DATETIME_DIFF(lead_estimate_time_kr, estimate_time_kr, SECOND) / 60 >= 5) THEN TRUE
         ELSE FALSE END is_valid_estimate_view
  FROM (
    SELECT
      estimation_request_joined_data.*,
      LEAD(estimate_time_kr) OVER (PARTITION BY estimation_request_joined_data.rider_id ORDER BY estimate_time_kr, request_time_kr) AS lead_estimate_time_kr,
      rb.call_view_type,
      rb.call_service_type,
      rb.type AS matched_type,
      rb.status,
      rb.cancellation_cause,
      rb.is_valid_5_last,
      rb.is_pure_session
    FROM
      estimation_request_joined_data LEFT JOIN (
        SELECT
          *
        FROM
          tada_store.ride_base
        WHERE date_kr BETWEEN "{target_date}" AND DATE_ADD("{target_date}", INTERVAL 1 DAY)
        ) AS rb
      USING(ride_id)
    )
  )

, ride_estimation_request AS (
  SELECT
    estimation_log_uuid,
    date_kr,
    estimate_time_kr,
    rider_id,
    origin_address,
    origin_lat,
    origin_lng,
    tada_udf.geo_to_h3(origin_lng, origin_lat, 7) AS origin_h3_l7,
    tada_udf.geo_to_h3(origin_lng, origin_lat, 9) AS origin_h3_l9,
    origin_region,
    destination_address,
    destination_lat,
    destination_lng,
    tada_udf.geo_to_h3(destination_lng, destination_lat, 7) AS destination_h3_l7,
    tada_udf.geo_to_h3(destination_lng, destination_lat, 7) AS destination_h3_l9,
    destination_region,
    distance_meters,
    duration_seconds,
    nxt_cost,
    lite_cost,
    plus_cost,
    nxt_surge_percentage,
    lite_surge_percentage,
    plus_surge_percentage,
    is_nxt_nearby_vehicle,
    is_lite_nearby_vehicle,
    is_plus_nearby_vehicle,
    request_time_kr,
    ride_id,
    original_ride_id,
    call_view_type,
    call_service_type,
    matched_type AS type,
    suggestion_type,
    status,
    cancellation_cause,
    is_pure_session,
    is_valid_5_last,
    is_valid_estimate_view,
    estimate_coupon_id,
    request_coupon_id
  FROM
    valid_view
  )

SELECT
  *
FROM
  ride_estimation_request