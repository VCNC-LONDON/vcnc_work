select
  count(1)
from
  tada.ride
where
  date_kr = '{execute_date}' + 1
  and created_at > timestamp_add('{execute_date} 00:00:00 Asia/Seoul', interval 300 second)