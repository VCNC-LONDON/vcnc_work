SELECT
  date_kr,
  etaAdjust.rideId AS ride_id,
  datetime(timestamp_millis(etaAdjust.originEtaAdjustInput.rideCreatedAtMs), 'Asia/Seoul') AS created_at_kr,
  datetime(timestamp_millis(etaAdjust.originEtaAdjustOutput.modelCreatedAtMs), 'Asia/Seoul') AS model_created_at_kr,
  round((cast(etaAdjust.originEtaAdjustOutput.etaDurationMs AS int64) / (1000*60)), 2) output_eta,
  round((cast(etaAdjust.originEtaAdjustInput.etaDurationMs AS int64) / (1000*60)), 2) input_eta,
  etaAdjust.originEtaAdjustInput.destinationGu AS input_origin_gu,
  etaAdjust.originEtaAdjustInput.originGu AS input_driver_gu,
  etaAdjust.originEtaAdjustInput.driverId AS input_driver_id
FROM
  tada.server_log_parquet
WHERE
  type = 'ML_ETA_GET_ADJUSTED_ORIGIN_ETA_DURATION'
  AND date_kr = '{execute_date}'