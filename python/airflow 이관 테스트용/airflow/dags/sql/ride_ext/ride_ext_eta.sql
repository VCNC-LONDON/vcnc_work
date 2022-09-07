WITH eta AS (
    SELECT
    upper(trim(ride.id)) AS ride_id,
    array_agg(
      if(
        ride.destinationEtaMs is null,
        struct(
          timestamp_millis(timeMs) AS time,
          timestamp_millis(ride.originEtaMs) AS eta,
          ride.status AS ride_status,
          ride.routeDistanceMeters AS route_distance_meters,
          ride.vehicleLocation.lng AS lng,
          ride.vehicleLocation.lat AS lat,
          ride.vehicleLocation.bearing AS bearing,
          timestamp_millis(ride.rawOriginEtaMs) AS origin_eta_raw,
          timestamp_millis(ride.etaModelCreatedAtMs) AS eta_model_created_at
        ),
        null
      )
      ignore nulls
      order by timeMs
    ) AS origin_eta,
    array_agg(
      if(
        ride.destinationEtaMs is not null,
        struct(
          timestamp_millis(timeMs) AS time,
          timestamp_millis(ride.destinationEtaMs) AS eta,
          ride.status AS ride_status,
          ride.routeDistanceMeters AS route_distance_meters,
          ride.vehicleLocation.lng AS lng,
          ride.vehicleLocation.lat AS lat,
          ride.vehicleLocation.bearing AS bearing,
          timestamp_millis(ride.rawOriginEtaMs) AS origin_eta_raw,
          timestamp_millis(ride.etaModelCreatedAtMs) AS eta_model_created_at
        ),
        null
      )
      ignore nulls
      order by timeMs
    ) AS destination_eta
  FROM
    tada.server_log_parquet
  WHERE
    type = 'DISPATCHER_NOTIFY_RIDE_ETA'
    AND date_kr BETWEEN '{execute_date}' AND '{execute_date}' + 1
  GROUP BY
    ride_id
)

SELECT
  id, 
  eta.origin_eta, 
  eta.destination_eta, 
  date_kr
FROM tada.ride
LEFT JOIN eta ON ride.id = eta.ride_id
WHERE date_kr = '{execute_date}'