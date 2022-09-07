select
  id,
  rn_rev = 1 or receipt_total is not null as is_valid1,
  date_kr
from (
  select
    id, receipt_total, date_kr,
    row_number() over (partition by rider_id, cast(floor(unix_seconds(created_at) / 600) as int64) order by created_at desc) as rn_rev
  from
    tada.ride
  where
    date_kr = '{execute_date}'
)