select
  timestamp_millis(timeMs) AS ts,
  etaLimitTestLog.rideId AS ride_id,
  timestamp_seconds(etaLimitTestLog.createdAt.epochSecond) AS match_context_created_at,
  etaLimitTestLog.driverId AS driver_id,
  etaLimitTestLog.vehicleId AS vehicle_id,
  etaLimitTestLog.reason,
  etaLimitTestLog.onDeprioritizingCrossroad AS on_deprioritizing_crossroad,
  etaLimitTestLog.scheduleEndAtComing AS is_schedule_end_at_coming,
  etaLimitTestLog.idx,
  etaLimitTestLog.estimatedRoi AS estimated_roi,
  etaLimitTestLog.etaDurationMs AS eta_duration_ms,
  etaLimitTestLog.etaFilterDurationMs AS eta_duration_ms_upper_limit,
  etaLimitTestLog.filteredByRoi AS filtered_by_roi,
  etaLimitTestLog.filteredByEta AS filtered_by_eta,
  etaLimitTestLog.rejectedByRoi AS rejected_by_roi,
  etaLimitTestLog.rejectedByEta AS rejected_by_eta,
  date_kr
FROM
  tada.server_log_parquet
WHERE
  type = 'ETA_LIMIT_TEST'
  AND date_kr = '{execute_date}'