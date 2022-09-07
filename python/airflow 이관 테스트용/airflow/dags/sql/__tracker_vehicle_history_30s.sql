select
  * except (rn)
from (
  select
    *,
    row_number() over (partition by vehicle_id, cast(floor(time_ms / 30000) as int64) order by time_ms) as rn
  from
    tada.tracker_vehicle_history
  where
    date_kr = '{execute_date}'
)
where
  rn = 1