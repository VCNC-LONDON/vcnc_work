select
  ride.* except (origin_eta, destination_eta, date_kr),

  ride_ext_is_valid.is_valid,

  ride_ext_eta.origin_eta,
  ride_ext_eta.destination_eta,

  ride_ext_ex.initial_origin_eta,
  ride_ext_ex.cancellation_fee_type,
  ride_ext_ex.should_waive_cancellation_fee,
  ride_ext_ex.biz_discount_amount,
  ride_ext_ex.biz_discount_rate,

  ride.date_kr
from
  tada.ride
  left join tada_ext.ride_ext_is_valid on ride.id = ride_ext_is_valid.id and ride_ext_is_valid.date_kr = '{execute_date}'
  left join tada_ext.ride_ext_eta on ride.id = ride_ext_eta.id and ride_ext_eta.date_kr = '{execute_date}'
  left join tada_ext.ride_ext_ex on ride.id = ride_ext_ex.id
where
  ride.date_kr = '{execute_date}'