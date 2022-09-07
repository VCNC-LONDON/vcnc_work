select
  id,
  safe_cast(json_value(data, '$.initialOriginEta') as int64) as initial_origin_eta,
  json_value(data, '$.cancellationFeeType') as cancellation_fee_type,
  safe_cast(json_value(data, '$.shouldWaiveCancellationFee') as boolean) as should_waive_cancellation_fee,
  safe_cast(json_value(data, '$.bizDiscountAmount') as int64) as biz_discount_amount,
  safe_cast(json_value(data, '$.bizDiscountRate') as numeric) as biz_discount_rate,
  ride.date_kr as date_kr
from
  tada.ride
  left join (SELECT * FROM tada.ride_ex WHERE date_kr BETWEEN "{execute_date}" AND "{execute_date}"+1) AS ride_ex on ride.id = ride_ex.ride_id
where
  ride.date_kr = "{execute_date}"