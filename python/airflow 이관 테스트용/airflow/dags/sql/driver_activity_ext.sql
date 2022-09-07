select
  driver_id,
  seq_id,
  activity_status,
  ride_status,
  ride_id,
  vehicle_id,
  start_at,
  end_at,
  lag(activity_status, 1) over (partition by driver_id order by seq_id) as activity_status_prev
from
  tada.driver_activity
