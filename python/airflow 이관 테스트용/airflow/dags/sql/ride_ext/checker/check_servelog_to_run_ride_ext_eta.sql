select
  count(1)
from
  tada.server_log_parquet
where
  date_kr = '{execute_date}' + 1
  and timeMs > (
    select 
        unix_millis(timestamp_add(max(dropped_off_at), interval 1 hour)) 
    from tada.ride 
    where date_kr = '{execute_date}'
)