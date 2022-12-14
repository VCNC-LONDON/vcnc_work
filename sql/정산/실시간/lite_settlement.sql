SELECT
  settlement_type,
  dropped_off_kst_date,
  ride_id,
  service_fee_promotion_type,
  agency_name,
  driver_name,
  driver_id,
  employee_number,
  business_type,
  business_license_number,
  license_plate,
  taxi_region_type,
  ride_cancelled_at_kr,
  ride_dropped_off_at_kr,
  payment_created_at_kr,
  payment_extra_type,
  pure_fare_fee,
  baseFee,
  timeBasedFee,
  distanceBasedFee,
  surcharged_fee,
  tollgate_fee,
  call_fee,
  cancellation_fee,
  extra_fee,
  total_fee,
  coupon_discount_amount,
  credit_discount_amount,
  employee_discount_amount,
  biz_discount_amount,
  bank_trasfer_amount,
  user_paid_amount,
  pg_service_fee,
  if (pg_name = 'TOSS_APP' or pg_name = 'EXIMBAY', vcnc_sales + pg_service_fee, vcnc_sales) as vcnc_sales,
  agency_deposit_amount,
  agency_payback,
  promotion_discount,
  pg_name,
  card_publisher_name,
  if (pg_name = 'TOSS_APP' and toss_app_pay_type = 'BANK', payment_transaction_id, pg_approval_number) as pg_approval_number,
  has_outstanding_balance,
  settlement_agency,
  tmoney_transaction_id,
  tmoney_processed_at,
  date(deposit_date) as deposit_date,
  eb_transaction_id,
  eb_processed_at
FROM base # baseline
WHERE ride_type = 'LITE'