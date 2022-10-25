# 얘도 참조하는 모든 테이블들에... 파티셔닝이 없어서 항상 full scan 이다.. (비용 줄이기가 안된다)
# record로 쌓는거니까 적어도 적재하는 결과만큼은..파티셔닝, 클러스터링 해놓고 쓸 수 있게 하자..

WITH 
payment_transaction AS (
    SELECT
        p.id AS payment_id,
        p.payment_method_id,
        pt.id AS payment_transaction_id,
        pt.payment_method_token_id, 
        pt.pg_approval_number,
        pm.toss_app_pay_type,
        pmt.type AS pg_name, -- 철자는 type 이지만 PG사 이름, PG사
        pmt.publisher_name AS card_publisher_name,
    FROM tada.payment_transaction pt
    LEFT JOIN tada.payment p ON p.id = pt.payment_id
    LEFT JOIN tada.payment_method_token pmt ON pt.payment_method_token_id = pmt.id
    LEFT JOIN tada.payment_method pm ON p.payment_method_id = pm.id
    WHERE pt.pg_approval_number IS NOT NULL  
    OR (pmt.type = 'TOSS_APP' AND pt.pg_approved_at IS NOT NULL)
),

taxi_settlement_info AS (
    SELECT
        service_fee_promotion_type,
        created_at,
        type,
        format_timestamp("%X", timestamp_millis(total_duration_ms)) AS total_duration,
        (total_distance_meters / 1000.0) AS total_distance_km,
        pure_fare_fee,
        CAST(JSON_EXTRACT(pricing_details, "$.ridePricingResult.basicFee") AS INT64) AS baseFee,
        CAST(JSON_EXTRACT(pricing_details, "$.ridePricingResult.timeBasedFee") AS INT64) AS timeBasedFee,
        CAST(JSON_EXTRACT(pricing_details, "$.ridePricingResult.distanceBasedFee") AS INT64) AS distanceBasedFee,
        surcharged_fee,
        tollgate_fee,
        call_fee,
        cancellation_fee,
        total_fee,
        if(payment_extra_type is NULL, 0, total_fee) AS extra_fee, -- 기타요금 항목에 영업손실비, 팁을 넣는다.
        CAST(JSON_EXTRACT(pricing_details, "$.ridePricingResult.discountAmount") AS INT64) AS coupon_discount_amount,
        CAST(JSON_EXTRACT(pricing_details, "$.ridePricingResult.creditAmount") AS INT64) AS credit_discount_amount,
        CAST(JSON_EXTRACT(pricing_details, "$.ridePricingResult.employeeDiscountAmount") AS INT64) AS employee_discount_amount,
        CAST(JSON_EXTRACT(pricing_details, "$.ridePricingResult.bizDiscountAmount") AS INT64) AS biz_discount_amount,
        user_paid_amount,
        vcnc_sales,
        agency_sales,
        agency_sales + tollgate_fee - IF(created_at > '2021-08-01' and is_outstanding_balance is false, raw_pg_service_fee, pg_service_fee) AS agency_deposit_amount,
        IF(created_at > '2021-08-01' and is_outstanding_balance is false, raw_pg_service_fee, pg_service_fee) AS pg_service_fee,
        (CASE service_fee_promotion_type
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
        END) AS agency_payback_rate,
        CAST(JSON_EXTRACT(pricing_details, "$.ridePricingResult.bankTransferAmount") AS INT64) AS bank_trasfer_amount,
        if(is_outstanding_balance = true, 'T', 'F') AS has_outstanding_balance, -- 미수발생여부(발생-T)
        ride_id,
        ride_reservation_id,
        trip_id,
        payment_id,
        driver_id, 
        vehicle_id, 
        agency_id, 
        type AS settlement_type,
        payment_extra_type,
        settlement_agency,
        tmoney_settlement_record_id,
        eb_settlement_record_id,
        is_outstanding_balance,
        is_cancelled,
        loan_repayment_fee
  from tada.taxi_settlement_info i
  -- where i.trip_id is null # 2에는 없는 조건
)

SELECT
    DATE(coalesce(r.dropped_off_at, r.cancelled_at), "Asia/Seoul") AS dropped_off_kst_date, -- 영업일자
    r.id AS ride_id, -- 라이드ID
    rr.id AS ride_reservation_id, -- 호출예약ID
    i.trip_id,
    i.service_fee_promotion_type, -- LITE_2020_SEOUL_1 같은 값이 들어감, eddie 시트에는 프로모션 적용 여부라고 적혀 있지만 promotion_type 을 적는다, 프로모션적용여부
    IF(i.driver_id IS NOT NULL, da.name, '없음') AS agency_name, -- 운수사
    IF(i.driver_id IS NOT NULL, d.name, '없음') AS driver_name, -- 드라이버 이름
    IF(i.driver_id IS NOT NULL, if(d.is_individual_business, '개인', '법인'), '-') AS business_type, -- 개인/법인 여부
    IF(i.vehicle_id IS NOT NULL, v.license_plate, '없음') AS license_plate, -- 차량번호
    IF(i.vehicle_id IS NOT NULL, IFNULL(v.taxi_region_type, 'SEOUL'), '-') AS taxi_region_type, -- 영업구역
    '' AS shift_ampm, -- XXX 못했음, 운수사에서 볼 요량으로 적는 듯 해서 필요한 듯 하기는 함, 오전/오후 근무
    tada_ext.address_mask(r.origin_address) AS origin_address_masked, -- 출발지
    tada_ext.address_mask(r.destination_address) AS destination_address_masked, -- 도착지
    DATETIME(r.cancelled_at, 'Asia/Seoul') AS ride_cancelled_at_kr, -- 콜취소시간
    DATETIME(r.accepted_at, 'Asia/Seoul') AS ride_accepted_at_kr, -- 콜응답시간
    DATETIME(r.picked_up_at, 'Asia/Seoul') AS ride_picked_up_at_kr, -- 탑승시간(이용자탑승시간)
    DATETIME(r.dropped_off_at, 'Asia/Seoul') AS ride_dropped_off_at_kr, -- 하차시간(이용자도착시간)
    DATETIME(i.created_at, 'Asia/Seoul') AS payment_created_at_kr, -- 결제시간
    i.total_duration, -- 운행시간(이용자 탑승후~도착)
    i.total_distance_km, -- 운행거리
    -- XXX 어떤 필드인지 잘 모르겠음. taxi_settlement_info 에서 더하고 빼서 구할 수 있는지도 잘 몰라요. 금액(할인적용전)
    IF(i.type = 'PAY', i.pure_fare_fee, -i.pure_fare_fee) AS pure_fare_fee, -- 기본운임(기본요금+시간+거리운임)
    IF(i.type = 'PAY', i.baseFee, -i.baseFee) AS baseFee, -- 기본 요금
    IF(i.type = 'PAY', i.timeBasedFee, -i.timeBasedFee) AS timeBasedFee, -- 시간운임(기본택시+할증택시 시간운임)
    IF(i.type = 'PAY', i.distanceBasedFee, -i.distanceBasedFee) AS distanceBasedFee, -- 거리운임(기본택시+할증택시 거리운임)
    IF(i.type = 'PAY', i.surcharged_fee, -i.surcharged_fee) AS surcharged_fee, -- 가맹서비스요금탄력요금제
    IF(i.type = 'PAY', i.cancellation_fee, -i.cancellation_fee) AS cancellation_fee, -- 취소수수료/미탑승수수료
    IF(i.type = 'PAY', i.tollgate_fee, -i.tollgate_fee) AS tollgate_fee,-- 통행료
    IF(i.type = 'PAY', i.call_fee, -i.call_fee) AS call_fee, -- 호출료
    IF(i.type = 'PAY', i.extra_fee, -i.extra_fee) AS extra_fee, -- 기타 요금 (팁, 영업손실비)
    IF(i.type = 'PAY', i.total_fee, -i.total_fee) AS total_fee, -- 총 금액
    IF(i.type = 'PAY', i.coupon_discount_amount, -i.coupon_discount_amount) AS coupon_discount_amount, -- 쿠폰
    IF(i.type = 'PAY', i.credit_discount_amount, -i.credit_discount_amount) AS credit_discount_amount, -- 크레딧
    IF(i.type = 'PAY', i.employee_discount_amount, -i.employee_discount_amount) AS employee_discount_amount, -- 직원할인
    IF(i.type = 'PAY', i.biz_discount_amount, -i.biz_discount_amount) AS biz_discount_amount, -- 비지니스할인
    IF(i.type = 'PAY', i.user_paid_amount, -i.user_paid_amount) AS user_paid_amount,-- 실결제가(A+B+C-D)
    IF(i.type = 'PAY', i.vcnc_sales, -i.vcnc_sales) AS vcnc_sales, -- VCNC매출
    IF(i.type = 'PAY', i.agency_sales, -agency_sales) AS agency_sales, -- 운수사매출
    IF(i.type = 'PAY', i.agency_deposit_amount, -agency_deposit_amount) AS agency_deposit_amount, -- 운수사 입금 금액
    (CASE
        WHEN i.driver_id IS NULL THEN 0
        WHEN d.is_individual_business IS TRUE THEN 0
        WHEN d.is_individual_business IS FALSE THEN IF(i.type = 'PAY', ROUND(i.pure_fare_fee * i.agency_payback_rate, -1), -ROUND(i.pure_fare_fee * i.agency_payback_rate, -1))
        ELSE 0
    END) AS agency_payback, -- 운수사 활동비
    (CASE
        WHEN i.driver_id IS NULL THEN 0
        WHEN d.is_individual_business IS TRUE THEN if(i.type = 'PAY', ROUND(i.pure_fare_fee * i.agency_payback_rate, -1), -ROUND(i.pure_fare_fee * i.agency_payback_rate, -1))
        WHEN d.is_individual_business IS FALSE THEN 0
        ELSE 0
    END) AS promotion_discount, -- 개인기사들에 대한 프로모션 감면액
    pt.pg_name, --PG사
    pt.card_publisher_name, -- pg에서 주는 값일터라 pg사 마다 같은 문자열을 쓴다는 보장은 없다, 카드사
    pt.pg_approval_number, -- 승인번호(카드승인),
    pt.toss_app_pay_type, -- 'CARD' or 'BANK',
    pt.payment_transaction_id, -- 주문번호
    IF(
      i.type = 'PAY', 
      IF(
        tsr IS NOT NULL AND NOT tsr.is_outstanding_balance AND pt.pg_name = 'KCP' AND pt.card_publisher_name != 'KB국민카드',
        ROUND(tsr.card_contribution * 0.0209, -1),
        i.pg_service_fee
      ),
      IF(
        tsr IS NOT NULL AND NOT tsr.is_outstanding_balance AND pt.pg_name = 'KCP' AND pt.card_publisher_name != 'KB국민카드',
        -round(tsr.card_contribution * 0.0209, -1),
        -i.pg_service_fee
      )
    ) AS pg_service_fee, -- PG 수수료
    IF(i.type = 'PAY', i.bank_trasfer_amount, -i.bank_trasfer_amount) AS bank_trasfer_amount, -- 계좌이체금액(어드민 입력)
    IF(i.is_outstanding_balance, 'T', 'F') AS has_outstanding_balance, -- 미수발생여부(발생-T)
    i.driver_id, 
    i.vehicle_id, 
    i.agency_id, 
    i.is_cancelled,
    IF(i.type = 'PAY', IFNULL(i.loan_repayment_fee, 0), -IFNULL(i.loan_repayment_fee, 0)) AS loan_repayment_fee, -- 대출상환액
    dti.business_license_number,
    dti.employee_number,
    IF(r.type is null, rr.ride_type, r.type) AS ride_type,
    i.type AS settlement_type,
    (CASE i.payment_extra_type
        WHEN 'LOSS' THEN '영업손실비'
        WHEN 'TIP' THEN '팁'
        ELSE '운임'
    END) AS payment_extra_type,
    IFNULL(i.settlement_agency, 'TMONEY') AS settlement_agency,
    tsr.tmoney_transaction_id,
    esr.id AS eb_transaction_id,
    DATETIME(tsr.tmoney_processed_at, 'Asia/Seoul') AS tmoney_processed_at,
    DATETIME(esr.eb_processed_at, 'Asia/Seoul') AS eb_processed_at,
    array_to_string(
        (CASE
            WHEN JSON_EXTRACT(tsr.details, '$.vcncSettlementDetail.vcncCardPaymentDetail.depositDate') IS NOT NULL 
            THEN JSON_EXTRACT_ARRAY(tsr.details, '$.vcncSettlementDetail.vcncCardPaymentDetail.depositDate')
            WHEN JSON_EXTRACT(tsr.details, '$.vcncSettlementDetail.realCardPaymentDetail.depositDate') IS NOT NULL 
            THEN JSON_EXTRACT_ARRAY(tsr.details, '$.vcncSettlementDetail.realCardPaymentDetail.depositDate')
            WHEN JSON_EXTRACT(tsr.details, '$.driverSettlementDetail.vcncCardPaymentDetail.depositDate') IS NOT NULL 
            THEN JSON_EXTRACT_ARRAY(tsr.details, '$.vcncSettlementDetail.vcncCardPaymentDetail.depositDate')
            ELSE JSON_EXTRACT_ARRAY(tsr.details, '$.vcncSettlementDetail.realCardPaymentDetail.depositDate')
        END),
        "-"
    ) AS deposit_date,
    c.id AS coupon_id, -- 쿠폰 아이디
    c.name AS coupon_name, -- 쿠폰 이름
    c.ride_campaign_id AS campaign_id, -- 캠페인 아이디
    (CASE c.benefit_type
        WHEN 'DISCOUNT_AMOUNT' THEN CONCAT(c.benefit_amount, '원')
        WHEN 'DISCOUNT_RATE' THEN CONCAT(c.benefit_amount, '%')
        ELSE ''
    END) AS coupon_benefit_info, -- 쿠폰 스킴
FROM taxi_settlement_info AS i
LEFT JOIN tada.tmoney_settlement_record AS tsr ON i.tmoney_settlement_record_id = tsr.id
LEFT JOIN tada.eb_settlement_record AS esr ON i.eb_settlement_record_id = esr.id
LEFT JOIN tada.ride AS r ON i.ride_id = r.id
LEFT JOIN tada.ride_reservation AS rr ON i.ride_reservation_id = rr.id
LEFT JOIN tada.driver AS d ON i.driver_id = d.id
LEFT JOIN tada.driver_tmoney_info AS dti ON d.tmoney_info_id = dti.id
LEFT JOIN payment_transaction AS pt ON i.payment_id = pt.payment_id and i.has_outstanding_balance = 'F'
LEFT JOIN tada.vehicle AS v ON i.vehicle_id = v.id
LEFT JOIN tada.driver_agency AS da ON d.agency_id = da.id
LEFT JOIN tada.ride_coupon AS c ON r.coupon_id = c.id