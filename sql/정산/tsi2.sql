(

with pt_and_p as (
  select p.id as payment_id,
         p.payment_method_id,
         pt.id as payment_transaction_id,
         pt.payment_method_token_id, 
         pt.pg_approval_number
  from tada.payment_transaction pt
    left outer join tada.payment p
    on p.id = pt.payment_id
  where pt.pg_approval_number is not null
)

select

  FORMAT_TIMESTAMP("%Y-%m-%d", coalesce(r.dropped_off_at, r.cancelled_at), "Asia/Seoul") as dropped_off_kst_date, -- 영업일자
  r.id as ride_id, -- 라이드ID
  i.service_fee_promotion_type, -- LITE_2020_SEOUL_1 같은 값이 들어감, eddie 시트에는 프로모션 적용 여부라고 적혀 있지만 promotion_type 을 적는다, 프로모션적용여부
  IF(i.driver_id IS NOT NULL, da.name, '없음') as agency_name, -- 운수사
  IF(i.driver_id IS NOT NULL, d.name, '없음') as driver_name, -- 드라이버 이름
  IF(i.vehicle_id IS NOT NULL, v.license_plate, '없음') as license_plate, -- 차량번호
  '' as shift_ampm, -- XXX 못했음, 운수사에서 볼 요량으로 적는 듯 해서 필요한 듯 하기는 함, 오전/오후 근무
  tada_ext.address_mask(r.origin_address) as origin_address_masked, -- 출발지
  tada_ext.address_mask(r.destination_address) as destination_address_masked, -- 도착지
  r.cancelled_at, -- 콜취소시간
  r.accepted_at, -- 콜응답시간
  r.picked_up_at, -- 탑승시간(이용자탑승시간)
  r.dropped_off_at, -- 하차시간(이용자도착시간)
  i.created_at, -- 결제시간
  format_timestamp("%X", timestamp_millis(i.total_duration_ms)) as total_duration, -- 운행시간(이용자 탑승후~도착)
  (i.total_distance_meters / 1000.0) as total_distance_km, -- 운행거리
  -- XXX 어떤 필드인지 잘 모르겠음. taxi_settlement_info 에서 더하고 빼서 구할 수 있는지도 잘 몰라요. 금액(할인적용전)
  i.pure_fare_fee, -- 기본운임(기본요금+시간+거리운임)
  JSON_EXTRACT(pricing_details, "$.ridePricingResult.basicFee") as baseFee, -- 기본 요금
  JSON_EXTRACT(pricing_details, "$.ridePricingResult.timeBasedFee") as timeBasedFee, -- 시간운임(기본택시+할증택시 시간운임)
  JSON_EXTRACT(pricing_details, "$.ridePricingResult.distanceBasedFee") as distanceBasedFee, -- 거리운임(기본택시+할증택시 거리운임)
  i.surcharged_fee, -- 가맹서비스요금탄력요금제
  i.tollgate_fee, -- 통행료
  i.call_fee, -- 호출료
  JSON_EXTRACT(pricing_details, "$.ridePricingResult.discountAmount") as coupon_discount_amount, -- 쿠폰
  JSON_EXTRACT(pricing_details, "$.ridePricingResult.employeeDiscountAmount") as employee_discount_amount, -- 직원할인
  JSON_EXTRACT(pricing_details, "$.ridePricingResult.bizDiscountAmount") as biz_discount_amount, -- 비지니스할인
  i.total_fee, -- tada_data repo 를 고쳐서 이제 생길 field 이다. 실결제가(A+B+C-D)
  i.vcnc_sales, -- VCNC매출
  i.agency_sales, -- 운수사매출
  -- 넣으면 혼란만 줄 필드라서 빼자, 모든게 검증된 값이라고 착각하기 쉽다, 금액검증
  -- XXX payment row 가 있으면 카드라고 봐도 되기는 하겠다. eddie 시트에는 예제로 (카드--계좌이체)가 적혀 있다, 결제수단
  pmt.type as pg_name, -- 철자는 type 이지만 PG사 이름, PG사
  pmt.publisher_name as card_publisher_name, -- pg에서 주는 값일터라 pg사 마다 같은 문자열을 쓴다는 보장은 없다, 카드사
  i.pg_service_fee as pg_eximbay_service_fee, -- 소스코드를 봐도, 두기한테 물어봐도 eximbay 일 때만 채우는 수수료가 분명하다 여튼 eddie 시트에 적힌 필드이름은 PG수수료 -- PG수수료
  pt_and_p.pg_approval_number, -- 승인번호(카드승인)
  "" as tmoney_fee, -- james 가 빈칸으로 넣으라 했음.
                    -- "건당이 아니라 한 번에 정산되는 금액에 총으로 0.5% 때림" 이라고 적혀 있다. 여기에 이 컬럼을 붙이는 것이, 원가 회계 측면에서나 적절하지, 여러모로 혼란만 일으킬 것 같다 ,T머니 수수료(금액)
  JSON_EXTRACT(pricing_details, "$.ridePricingResult.bankTransferAmount") as bank_trasfer_amount, -- 계좌이체금액(어드민 입력)
  if(i.is_outstanding_balance = true, 'T', 'F') as has_outstanding_balance, -- 미수발생여부(발생-T)
  "" as misu_resolution_timestamp,  -- 빈칸, 잘 모르겠음. 미수해결시각
  "" as payback_amount, -- 빈칸, 내가 안 함. 
        --   혹시 (agency_id, promotion_service_type) tutple 마다 payback_rate 를 긴 case when 문으로 만들어두고,
        --   데이터 제공 및 광고에 대한 지원금 (페이백)
  i.driver_id, 
  i.vehicle_id, 
  i.agency_id, 
  
  i.cancellation_fee, 
  i.user_paid_amount, 
  i.vcnc_paid_amount,
  r.type as ride_type,
  i.type as settlement_type,
  i.payment_extra_type as payment_extra_type,
  i.settlement_agency as settlement_agency,

from tada.taxi_settlement_info i

left outer join tada.ride r on i.ride_id = r.id
left outer join tada.driver d on i.driver_id = d.id
left outer join pt_and_p on i.payment_id = pt_and_p.payment_id
 left outer join tada.payment_method_token pmt on pt_and_p.payment_method_token_id = pmt.id
left outer join tada.vehicle v on i.vehicle_id = v.id
 left outer join tada.driver_agency da on v.agency_id = da.id

)