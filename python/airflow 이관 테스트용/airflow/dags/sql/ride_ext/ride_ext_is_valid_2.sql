select
  id,
  status = 'DROPPED_OFF' or diff_sec_to_next > 300 or diff_sec_to_next is null as is_valid2,
  date_kr
from (
  select
    id, created_at, status, date_kr,
    timestamp_diff(lead(created_at, 1) over (partition by type, rider_id order by created_at), created_at, second) as diff_sec_to_next
  from
    tada.ride
  where
    date_kr between '{execute_date}' and '{execute_date}' + 1
)
where
  date_kr = '{execute_date}'