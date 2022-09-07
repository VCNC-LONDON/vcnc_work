SELECT
  -- 로그 정보
  uuid AS log_uuid,
  type AS log_type,
  timeMs AS time_ms,
  DATETIME(TIMESTAMP_MILLIS(timeMs), 'Asia/Seoul') AS time_kr,
  
  -- estimation_uuid (ride_estimation_ext 과의 join key)
  ride.estimation.estimationUuid as estimation_uuid,
  
  -- 고객 정보
  IFNULL(caller.userId, ride.riderId) AS rider_id,
  caller.userAgent AS user_agent,
  caller.appsflyerId AS appsflyer_id,
  caller.gaid AS ga_id,
  caller.idfa AS fa_id,
  
  -- 호출 정보
  ride.id AS ride_id,
  ride.rideType AS ride_type,
  ride.status AS ride_status,
  ride.createdAtMs AS ride_created_at_ms,
  DATETIME(TIMESTAMP_MILLIS(ride.createdAtMs), 'Asia/Seoul') as ride_created_at_kr,
  ride.vehicleId AS vehicle_id,
  ride.driverId AS driver_id,
  
  -- 여정 정보
  ride.origin.lat AS origin_lat,
  ride.origin.lng AS origin_lng,
  ride.origin.name AS origin_name,
  ride.origin.address AS origin_address,
  ride.destination.lat AS destination_lat,
  ride.destination.lng AS destination_lng,
  ride.destination.name AS destination_name,
  ride.destination.address AS destination_address,
  ride.originSource AS origin_source,
  ride.destinationSource AS destination_source,
  ride.routeUuid AS route_uuid,
  ride.estimation.distanceMeters AS distance_meters,
  ride.estimation.durationSeconds AS duration_seconds,
  
  -- 넛지 정보
  ride.suggestionType AS suggestion_type,
  ride.nearRideLog.cause AS near_ride_cause,
  ride.nearRideLog.determinedRideType AS determined_ride_type,
  ride.originalRideId AS original_ride_id,
   
  -- 가격 정보
  ride.estimation.minCost AS min_cost,
  ride.estimation.maxCost AS max_cost,

  -- 쿠폰 정보
  ride.couponId AS coupon_id,
  
  date_kr
FROM tada.server_log_parquet
WHERE date_kr = '{target_date}'
  AND type IN ('USER_REQUEST_RIDE', 'USER_REQUEST_ALTERNATIVE_RIDE', 'SERVER_RETRY_RIDE')