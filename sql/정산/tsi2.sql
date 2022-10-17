(

with 
  pt_and_p as (
    select 
      p.id as payment_id,
      p.payment_method_id,
      pt.id as payment_transaction_id,
      pt.payment_method_token_id, 
      pt.pg_approval_number,
      pm.toss_app_pay_type,
      pmt.type as pg_name, -- 철자는 type 이지만 PG사 이름, PG사
      pmt.publisher_name as card_publisher_name,
    from tada.payment_transaction pt
      left outer join tada.payment p on p.id = pt.payment_id
      left outer join tada.payment_method_token pmt on pt.payment_method_token_id = pmt.id
      left outer join tada.payment_method pm on p.payment_method_id = pm.id
    where pt.pg_approval_number is not null  or (pmt.type = 'TOSS_APP' and pt.pg_approved_at is not null)
  ),

  i as (
    select
      i.service_fee_promotion_type,
      i.created_at,
      i.type,
      format_timestamp("%X", timestamp_millis(i.total_duration_ms)) as total_duration,
      (i.total_distance_meters / 1000.0) as total_distance_km,
      i.pure_fare_fee,
      CAST(JSON_EXTRACT(pricing_details, "$.ridePricingResult.basicFee") AS INT64) as baseFee,
      CAST(JSON_EXTRACT(pricing_details, "$.ridePricingResult.timeBasedFee") AS INT64) as timeBasedFee,
      CAST(JSON_EXTRACT(pricing_details, "$.ridePricingResult.distanceBasedFee") AS INT64) as distanceBasedFee,
      i.surcharged_fee,
      i.tollgate_fee,
      i.call_fee,
      i.cancellation_fee,
      i.total_fee,
      if(i.payment_extra_type is NULL, 0, i.total_fee) as extra_fee, -- 기타요금 항목에 영업손실비, 팁을 넣는다.
      CAST(JSON_EXTRACT(pricing_details, "$.ridePricingResult.discountAmount") AS INT64) as coupon_discount_amount,
      CAST(JSON_EXTRACT(pricing_details, "$.ridePricingResult.creditAmount") AS INT64) as credit_discount_amount,
      CAST(JSON_EXTRACT(pricing_details, "$.ridePricingResult.employeeDiscountAmount") AS INT64) as employee_discount_amount,
      CAST(JSON_EXTRACT(pricing_details, "$.ridePricingResult.bizDiscountAmount") AS INT64) as biz_discount_amount,
      i.user_paid_amount,
      i.vcnc_sales,
      i.agency_sales,
      i.agency_sales + i.tollgate_fee - IF(i.created_at > '2021-08-01' and i.is_outstanding_balance is false, i.raw_pg_service_fee, i.pg_service_fee) as agency_deposit_amount,
      IF(i.created_at > '2021-08-01' and i.is_outstanding_balance is false, i.raw_pg_service_fee, i.pg_service_fee) as pg_service_fee,
      CASE i.service_fee_promotion_type
        WHEN 'LITE_2020_SEOUL_1' then 0.13
        WHEN 'LITE_2020_SEONGNAM_1' then 0.125
        WHEN 'LITE_2020_SEONGNAM_A' then 0.130
        WHEN 'LITE_2020_SEONGNAM_B' then 0.117
        WHEN 'LITE_2020_SEONGNAM_C' then 0.125
        WHEN 'LITE_2020_DONGSEOUL' then 0.135
        WHEN 'LITE_2020_BUSAN' then 0.11
        WHEN 'LITE_2021_INDIVIDUAL_SEOUL_1' then 0.1
        WHEN 'LITE_2021_SEOUL_3' then 0.13
        WHEN 'LITE_2021_INDIVIDUAL_SEOUL_2' then 0.15
        WHEN 'LITE_2021_BUSAN_2' then 0.16
        ELSE 0
      END AS agency_payback_rate,
      CAST(JSON_EXTRACT(pricing_details, "$.ridePricingResult.bankTransferAmount") AS INT64) as bank_trasfer_amount,
      if(i.is_outstanding_balance = true, 'T', 'F') as has_outstanding_balance, -- 미수발생여부(발생-T)
      i.ride_id,
      i.payment_id,
      i.driver_id, 
      i.vehicle_id, 
      i.agency_id, 
      i.type as settlement_type,
      i.payment_extra_type,
      i.settlement_agency,
      i.tmoney_settlement_record_id,
      i.eb_settlement_record_id,
      i.is_outstanding_balance,
      i.is_cancelled
    from tada.taxi_settlement_info i
  )

select
  FORMAT_TIMESTAMP("%Y-%m-%d", coalesce(r.dropped_off_at, r.cancelled_at), "Asia/Seoul") as dropped_off_kst_date, -- 영업일자
  r.id as ride_id, -- 라이드ID
  i.service_fee_promotion_type, -- LITE_2020_SEOUL_1 같은 값이 들어감, eddie 시트에는 프로모션 적용 여부라고 적혀 있지만 promotion_type 을 적는다, 프로모션적용여부
  IF(i.driver_id IS NOT NULL, da.name, '없음') as agency_name, -- 운수사
  IF(i.driver_id IS NOT NULL, d.name, '없음') as driver_name, -- 드라이버 이름
  IF(i.driver_id IS NOT NULL, if(d.is_individual_business, '개인', '법인'), '-') as business_type, -- 개인/법인 여부
  IF(i.vehicle_id IS NOT NULL, v.license_plate, '없음') as license_plate, -- 차량번호
  IF(i.vehicle_id IS NOT NULL, IFNULL(v.taxi_region_type, 'SEOUL'), '-') as taxi_region_type, -- 영업구역
  '' as shift_ampm, -- XXX 못했음, 운수사에서 볼 요량으로 적는 듯 해서 필요한 듯 하기는 함, 오전/오후 근무
  tada_ext.address_mask(r.origin_address) as origin_address_masked, -- 출발지
  tada_ext.address_mask(r.destination_address) as destination_address_masked, -- 도착지
  DATETIME(r.cancelled_at, 'Asia/Seoul') as cancelled_at, -- 콜취소시간
  DATETIME(r.accepted_at, 'Asia/Seoul') as accepted_at, -- 콜응답시간
  DATETIME(r.picked_up_at, 'Asia/Seoul') as picked_up_at, -- 탑승시간(이용자탑승시간)
  DATETIME(r.dropped_off_at, 'Asia/Seoul') as dropped_off_at, -- 하차시간(이용자도착시간)
  DATETIME(i.created_at, 'Asia/Seoul') as created_at, -- 결제시간
  i.total_duration, -- 운행시간(이용자 탑승후~도착)
  i.total_distance_km, -- 운행거리
  -- XXX 어떤 필드인지 잘 모르겠음. taxi_settlement_info 에서 더하고 빼서 구할 수 있는지도 잘 몰라요. 금액(할인적용전)
  if(i.type = 'PAY', i.pure_fare_fee, -i.pure_fare_fee) as pure_fare_fee, -- 기본운임(기본요금+시간+거리운임)
  if(i.type = 'PAY', i.baseFee, -i.baseFee) as baseFee, -- 기본 요금
  if(i.type = 'PAY', i.timeBasedFee, -i.timeBasedFee) as timeBasedFee, -- 시간운임(기본택시+할증택시 시간운임)
  if(i.type = 'PAY', i.distanceBasedFee, -i.distanceBasedFee) as distanceBasedFee, -- 거리운임(기본택시+할증택시 거리운임)
  if(i.type = 'PAY', i.surcharged_fee, -i.surcharged_fee) as surcharged_fee, -- 가맹서비스요금탄력요금제
  if(i.type = 'PAY', i.cancellation_fee, -i.cancellation_fee) as cancellation_fee, -- 취소수수료/미탑승수수료
  if(i.type = 'PAY', i.tollgate_fee, -i.tollgate_fee) as tollgate_fee,-- 통행료
  if(i.type = 'PAY', i.call_fee, -i.call_fee) as call_fee, -- 호출료
  if(i.type = 'PAY', i.extra_fee, -i.extra_fee) as extra_fee, -- 기타 요금 (팁, 영업손실비)
  if(i.type = 'PAY', i.total_fee, -i.total_fee) as total_fee, -- 총 금액
  if(i.type = 'PAY', i.coupon_discount_amount, -i.coupon_discount_amount) as coupon_discount_amount, -- 쿠폰
  if(i.type = 'PAY', i.credit_discount_amount, -i.credit_discount_amount) as credit_discount_amount, -- 크레딧
  if(i.type = 'PAY', i.employee_discount_amount, -i.employee_discount_amount) as employee_discount_amount, -- 직원할인
  if(i.type = 'PAY', i.biz_discount_amount, -i.biz_discount_amount) as biz_discount_amount, -- 비지니스할인
  if(i.type = 'PAY', i.user_paid_amount, -i.user_paid_amount) as user_paid_amount,-- 실결제가(A+B+C-D)
  if(i.type = 'PAY', i.vcnc_sales, -i.vcnc_sales) as vcnc_sales, -- VCNC매출
  if(i.type = 'PAY', i.agency_sales, -agency_sales) as agency_sales, -- 운수사매출
  if(i.type = 'PAY', i.agency_deposit_amount, -agency_deposit_amount) as agency_deposit_amount, -- 운수사 입금 금액
  CASE
   WHEN i.driver_id IS NULL THEN 0
   WHEN d.is_individual_business IS TRUE THEN 0
   WHEN d.is_individual_business IS FALSE THEN if(i.type = 'PAY', round(i.pure_fare_fee * i.agency_payback_rate, -1), -round(i.pure_fare_fee * i.agency_payback_rate, -1))
   ELSE 0
  END as agency_payback, -- 운수사 활동비
  CASE
   WHEN i.driver_id IS NULL THEN 0
   WHEN d.is_individual_business IS TRUE THEN if(i.type = 'PAY', round(i.pure_fare_fee * i.agency_payback_rate, -1), -round(i.pure_fare_fee * i.agency_payback_rate, -1))
   WHEN d.is_individual_business IS FALSE THEN 0
   ELSE 0
  END as promotion_discount, -- 개인기사들에 대한 프로모션 감면액
  pt_and_p.pg_name, --PG사
  pt_and_p.card_publisher_name, -- pg에서 주는 값일터라 pg사 마다 같은 문자열을 쓴다는 보장은 없다, 카드사
  pt_and_p.pg_approval_number, -- 승인번호(카드승인),
  pt_and_p.toss_app_pay_type, -- 'CARD' or 'BANK',
  pt_and_p.payment_transaction_id, -- 주문번호
  if(
    i.type = 'PAY', 
    if(
      tsr is not null and tsr.is_outstanding_balance is false and pt_and_p.pg_name = 'KCP' and pt_and_p.card_publisher_name != 'KB국민카드',
      round(tsr.card_contribution * 0.0209, -1),
      i.pg_service_fee
    ),
    if(
      tsr is not null and tsr.is_outstanding_balance is false and pt_and_p.pg_name = 'KCP' and pt_and_p.card_publisher_name != 'KB국민카드',
      -round(tsr.card_contribution * 0.0209, -1),
      -i.pg_service_fee
    )
  ) as pg_service_fee, -- PG 수수료
  if(i.type = 'PAY', i.bank_trasfer_amount, -i.bank_trasfer_amount) as bank_trasfer_amount, -- 계좌이체금액(어드민 입력)
  if(i.is_outstanding_balance = true, 'T', 'F') as has_outstanding_balance, -- 미수발생여부(발생-T)
  i.driver_id, 
  i.vehicle_id, 
  i.agency_id, 
  i.is_cancelled,
  dti.business_license_number,
  dti.employee_number,
  
  r.type as ride_type,
  i.type as settlement_type,
  CASE i.payment_extra_type
    WHEN 'LOSS' THEN '영업손실비'
    WHEN 'TIP' THEN '팁'
    ELSE '운임'
  END AS payment_extra_type,
  ifnull(i.settlement_agency, 'TMONEY') as settlement_agency,
  tsr.tmoney_transaction_id,
  esr.id as eb_transaction_id,
  DATETIME(tsr.tmoney_processed_at, 'Asia/Seoul') as tmoney_processed_at,
  DATETIME(esr.eb_processed_at, 'Asia/Seoul') as eb_processed_at,
  array_to_string(
  CASE
    WHEN JSON_EXTRACT(tsr.details, '$.vcncSettlementDetail.vcncCardPaymentDetail.depositDate') IS NOT NULL THEN
      JSON_EXTRACT_ARRAY(tsr.details, '$.vcncSettlementDetail.vcncCardPaymentDetail.depositDate')
    WHEN JSON_EXTRACT(tsr.details, '$.vcncSettlementDetail.realCardPaymentDetail.depositDate') IS NOT NULL THEN
      JSON_EXTRACT_ARRAY(tsr.details, '$.vcncSettlementDetail.realCardPaymentDetail.depositDate')
    WHEN JSON_EXTRACT(tsr.details, '$.driverSettlementDetail.vcncCardPaymentDetail.depositDate') IS NOT NULL THEN
      JSON_EXTRACT_ARRAY(tsr.details, '$.vcncSettlementDetail.vcncCardPaymentDetail.depositDate')
    ELSE
      JSON_EXTRACT_ARRAY(tsr.details, '$.vcncSettlementDetail.realCardPaymentDetail.depositDate')
    END,
    "-"
  ) AS deposit_date,
  c.id as coupon_id, -- 쿠폰 아이디
  c.name as coupon_name, -- 쿠폰 이름
  c.ride_campaign_id as campaign_id, -- 캠페인 아이디
  CASE c.benefit_type
    WHEN 'DISCOUNT_AMOUNT' THEN CONCAT(c.benefit_amount, '원')
    WHEN 'DISCOUNT_RATE' THEN CONCAT(c.benefit_amount, '%')
    ELSE ''
  END as coupon_benefit_info, -- 쿠폰 스킴

from i
  left outer join tada.tmoney_settlement_record tsr on i.tmoney_settlement_record_id = tsr.id
  left outer join tada.eb_settlement_record esr on i.eb_settlement_record_id = esr.id
  left outer join tada.ride r on i.ride_id = r.id
  left outer join tada.driver d on i.driver_id = d.id
  left outer join tada.driver_tmoney_info dti on d.tmoney_info_id = dti.id
  left outer join pt_and_p on i.payment_id = pt_and_p.payment_id and i.has_outstanding_balance = 'F'
  left outer join tada.vehicle v on i.vehicle_id = v.id
  left outer join tada.driver_agency da on d.agency_id = da.id
  left outer join tada.ride_coupon c on r.coupon_id = c.id
)