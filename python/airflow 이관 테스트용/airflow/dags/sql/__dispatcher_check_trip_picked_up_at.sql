WITH base AS (
    SELECT
      etaCheckingTripPickedUpAt.rideId AS ride_id,
      timestamp_millis(etaCheckingTripPickedUpAt.matchContextCreatedAtMs) AS match_context_created_at,
      etaCheckingTripPickedUpAt.driverId AS driver_id,
      etaCheckingTripPickedUpAt.driverLng AS driver_lng,
      etaCheckingTripPickedUpAt.driverLat AS driver_lat,
      etaCheckingTripPickedUpAt.tripId AS trip_id,
      timestamp_millis(etaCheckingTripPickedUpAt.tripPickedUpAtMs) AS trip_picked_up_at,
      timestamp_millis(etaCheckingTripPickedUpAt.timeMs) AS ts,
      etaCheckingTripPickedUpAt.tripOriginEtaDurationMs AS trip_origin_eta_duration_ms,
      etaCheckingTripPickedUpAt.bufferDurationMs AS buffer_duration_ms,
      date_kr
    FROM
      tada.server_log_parquet
    WHERE
      type = 'DISPATCHER_CHECK_TRIP_PICKED_UP_AT'
      AND date_kr BETWEEN "{execute_date}"-1 AND "{execute_date}"
  )


SELECT
  * EXCEPT (rn)
FROM (
  SELECT
    *,
    row_number() over (partition by ride_id, match_context_created_at, driver_id order by ts) AS rn
  FROM base
)
WHERE
  rn = 1
  AND date_kr = '{execute_date}'