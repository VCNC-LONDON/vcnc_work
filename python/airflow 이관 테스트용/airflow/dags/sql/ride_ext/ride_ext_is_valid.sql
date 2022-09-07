select
  ride.id,
  case
    when ride.date_kr between '2018-10-08' and '2019-04-11' then is_valid1
    when ride.date_kr >= '2019-04-12' then is_valid2
  end as is_valid,
  ride.date_kr
from
  tada.ride
  left join tada_ext.ride_ext_is_valid1 r1 on ride.id = r1.id and r1.date_kr = '{execute_date}'
  left join tada_ext.ride_ext_is_valid2 r2 on ride.id = r2.id and r2.date_kr = '{execute_date}'
where
  ride.date_kr = '{execute_date}'