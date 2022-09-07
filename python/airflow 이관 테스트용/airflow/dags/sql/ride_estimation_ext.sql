SELECT
  -- 로그 정보
  uuid AS log_uuid,
  type AS log_type,
  timeMs AS time_ms,
  DATETIME(TIMESTAMP_MILLIS(timeMs), 'Asia/Seoul') AS time_kr,

  -- estimation_uuid (ride_request 와의 join key)
  rideEstimation.estimationUuid AS estimation_uuid,

  -- 고객 정보
  IFNULL(caller.userId, ride.riderId) AS rider_id,
  caller.userAgent AS user_agent,
  caller.appsflyerId AS appsflyer_id,
  caller.gaid AS ga_id,
  caller.idfa AS fa_id,
  
  -- rideEstimation 정보
  rideEstimation.rideType AS estimation_ride_type,
  alternativeRideTypes.list AS alternative_ride_types_list,
  rideEstimation.liteUnavailableByNoNearbyVehicle AS lite_unavailable_by_no_nearby_vehicle,
  rideEstimation.unavailableByNoNearbyVehicleRideTypes AS unavailable_by_no_nearby_vehicle_list,
  rideEstimation.success AS estimation_success,

  -- 여정 정보
  rideEstimation.origin.lat AS origin_lat,
  rideEstimation.origin.lng AS origin_lng,
  rideEstimation.origin.name AS origin_name,
  rideEstimation.origin.address AS origin_address,
  rideEstimation.destination.lat AS destination_lat,
  rideEstimation.destination.lng AS destination_lng,
  rideEstimation.destination.name AS destination_name,
  rideEstimation.destination.address AS destination_address,
  rideEstimation.routeUuid AS route_uuid,
  rideEstimation.distanceMeters AS distance_meters,
  rideEstimation.durationSeconds AS duration_seconds,

  -- 가격 및 서지 정보
  rideEstimation.minCost AS min_cost,
  rideEstimation.maxCost AS max_cost,
  rideEstimation.surgePercentage AS surge_percentage,
  rideEstimation.surgePolicyId AS surge_policy_id,
  rideEstimation.priceElASticityTestingPercentage AS price_elasticity_test_percentage,

  -- 쿠폰 정보
  rideEstimation.couponId AS coupon_id,
    
  -- 파티션 date_kr
  date_kr
  
FROM tada.server_log_parquet
WHERE date_kr  = "{target_date}"
  AND type IN ('USER_ESTIMATE_RIDE', 'DISPATCHER_ALTERNATIVE_RIDE_SUGGESTION')