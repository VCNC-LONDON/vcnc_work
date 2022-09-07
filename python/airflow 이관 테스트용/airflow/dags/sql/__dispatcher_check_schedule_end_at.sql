WITH base AS (
    SELECT
      etaCheckingScheduleEndAt.rideId as ride_id,
      timestamp_millis(etaCheckingScheduleEndAt.matchContextCreatedAtMs) as match_context_created_at,
      etaCheckingScheduleEndAt.driverId as driver_id,
      etaCheckingScheduleEndAt.driverLng as driver_lng,
      etaCheckingScheduleEndAt.driverLat as driver_lat,
      etaCheckingScheduleEndAt.vehicleZoneId as vehicle_zone_id,
      timestamp_millis(etaCheckingScheduleEndAt.scheduleEndAtMs) as schedule_end_at,
      timestamp_millis(etaCheckingScheduleEndAt.timeMs) as ts,
      etaCheckingScheduleEndAt.vehicleZoneEtaDurationMs as vehicle_zone_eta_duration_ms,
      etaCheckingScheduleEndAt.bufferDurationMs as buffer_duration_ms,
      date_kr
    FROM
      tada.server_log_parquet
    WHERE
      type = 'DISPATCHER_CHECK_SCHEDULE_END_AT'
      AND date_kr BETWEEN "{execute_date}"-1 AND "{execute_date}" -- 중복 제거할 때 첫번째 것을 취하므로, 1일 전 까지 봅니다.
  )


SELECT
    * EXCEPT (rn)
FROM (
  SELECT
    *,
    row_number() over (PARTITION BY ride_id, match_context_created_at, driver_id ORDER BY ts) AS rn
  FROM base
)
WHERE
  rn = 1
  AND date_kr = "{execute_date}"