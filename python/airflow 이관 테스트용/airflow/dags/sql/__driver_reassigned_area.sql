SELECT
  uuid,
  date_kr,
  datetime(timestamp_trunc(timestamp_millis(timeMs), second), 'Asia/Seoul') as time_kr,
  timeMs as time_ms,
  reassignAreaTask.assignedAreaCandidates as assigned_area_candidates,
  reassignAreaTask.dispatchingDriverCount as dispatching_driver_cnt,
  reassignAreaTask.driverId as driver_id,
  reassignAreaTask.lastRideId as last_ride_id,
  reassignAreaTask.regionBlockCandidates as region_block_candidates,
  reassignAreaTask.searchDistanceMeters as search_distance_meters,
  reassignAreaTask.selectedAssignedArea.lat as selected_assigned_area_lat,
  reassignAreaTask.selectedAssignedArea.lng as selected_assigned_area_lng,
  reassignAreaTask.selectedAssignedArea.name as selected_assigned_area_name,
  reassignAreaTask.selectedRegionBlock.id as selected_region_block_id,
  reassignAreaTask.workingDriverCount as working_driver_cnt
FROM
  tada.server_log_parquet
WHERE
  date_kr = '{execute_date}'
  and type = 'SERVER_REASSIGN_AREA_TASK'