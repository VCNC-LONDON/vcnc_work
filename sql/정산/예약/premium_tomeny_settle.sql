# 얘는 셈법이나 조건이 특이해서 어쩔 수 없이 따로 써야 한다

WITH

base AS (
  SELECT
    DATE(ifnull(trip.dropped_off_at, trip.cancelled_at), 'Asia/Seoul') AS service_fee_date,
    DATETIME(tsr.transacted_at, 'Asia/Seoul') AS transacted_at_kr,
    DATE(ARRAY_TO_STRING((CASE
      WHEN JSON_EXTRACT(tsr.details, '$.vcncSettlementDetail.vcncCardPaymentDetail.depositDate') IS NOT NULL
      THEN JSON_EXTRACT_ARRAY(tsr.details, '$.vcncSettlementDetail.vcncCardPaymentDetail.depositDate')

      WHEN JSON_EXTRACT(tsr.details, '$.vcncSettlementDetail.realCardPaymentDetail.depositDate') IS NOT NULL
      THEN JSON_EXTRACT_ARRAY(tsr.details, '$.vcncSettlementDetail.realCardPaymentDetail.depositDate')

      WHEN JSON_EXTRACT(tsr.details, '$.driverSettlementDetail.vcncCardPaymentDetail.depositDate') IS NOT NULL
      THEN JSON_EXTRACT_ARRAY(tsr.details, '$.vcncSettlementDetail.vcncCardPaymentDetail.depositDate')

      ELSE JSON_EXTRACT_ARRAY(tsr.details, '$.vcncSettlementDetail.realCardPaymentDetail.depositDate')
    END),"-")) AS deposit_date,
    tsr.tmoney_transaction_id AS tmoney_transaction_id,
    IF(tsr.is_outstanding_balance, 'Y', 'N') AS is_outstanding_balance,
    IF(trip.should_waive_service_fee, 'Y', 'N') AS should_waive_service_fee,
    IF(driver.is_individual_business, '개인', driver_agency.name) AS agency_name,
    (CASE tsr.settlement_type
      WHEN 'PAY' then '결제'
      ELSE '환불'
    END) AS payment_type,
    (CASE tsr.payment_extra_type
      WHEN 'TRIP_ADDITIONAL' THEN '추가요금'
      WHEN 'TIP' THEN '팁'
      ELSE '운임'
    END) AS payment_reason,
    vehicle.license_plate AS license_plate,
    driver.id AS driver_id,
    driver.name AS driver_name,
    trip.id AS trip_id,
    IF(trip.reservation_type = 'B2B', 'B2B', 'B2C') AS trip_b2x,
    (CASE
      WHEN trip.type = 'CHARTER' THEN 'PRIVATE'

      WHEN trip.type = 'AIR' THEN CONCAT(
        'AIR_',
        CASE air_airport_code
          WHEN '0' THEN 'ICN'
          WHEN '1' THEN 'ICN'
          WHEN '2' THEN 'GMP'
          WHEN '3' THEN 'GMP'
          ELSE 'XXX'
        END,
        '_',
        CASE air_type
          WHEN '0' THEN 'SENDING'
          WHEN '1' THEN 'PICKUP'
          ELSE 'XXX'
        END
      )

      ELSE trip.type
    END) AS trip_type,
    tsr.pay_amount AS pay_amount,
    tsr.meter_amount AS meter_amount,
    tsr.tollgate_fee AS tollgate_fee,
    (CASE
      WHEN payment_method_token.type = 'EXIMBAY' 
      OR payment.created_at < '2020-09-02 15:00:00' 
      OR (
        payment_method.card_publisher = 'KB국민카드' 
        AND (tsr.payment_extra_type IS NULL OR tsr.payment_extra_type = 'RECHARGE')
      ) 
      THEN IF(payment_transaction.amount IS NULL, 0, payment_transaction.amount - tsr.tollgate_fee)

      WHEN tsr.pay_amount = tsr.vcnc_contribution 
      THEN tsr.pay_amount - tsr.vcnc_contribution

      ELSE tsr.meter_amount - tsr.vcnc_contribution
    END) AS card_contribution,
    (
      tsr.vcnc_contribution 
      - IF(
      tsr.payment_extra_type = 'TRIP_ADDITIONAL',
      IFNULL(trip.additional_receipt_bank_transfer_amount, 0),
      0
      ) 
      - (
        CASE
          WHEN payment_method_token.type = 'EXIMBAY' 
          OR payment.created_at < '2020-09-02 15:00:00' 
          OR (
            payment_method.card_publisher = 'KB국민카드' 
            AND (tsr.payment_extra_type is null or tsr.payment_extra_type = 'RECHARGE')
          ) 
          THEN IFNULL(payment_transaction.amount, 0)

          WHEN payment.created_at < '2020-09-02 15:00:00' 
          THEN IFNULL(payment_transaction.amount, 0)

          WHEN tsr.pay_amount = tsr.vcnc_contribution 
          THEN tsr.tollgate_fee

          ELSE 0
        END
      ) 
    ) AS vcnc_contribution,
    trip.receipt_coupon_discount AS coupon_discount,
    IFNULL(trip.receipt_biz_discount, 0) AS biz_discount,
    IF(tsr.payment_extra_type = 'TIP', 0, CAST(tsr.meter_amount * 0.1 AS INT64)) AS service_fee,
    tsr.service_fee AS vcnc_amount,
    IF(
      tsr.payment_extra_type = 'TRIP_ADDITIONAL',
      IF(
        IFNULL(trip.additional_receipt_bank_transfer_amount, 0) = tsr.pay_amount,
        IFNULL(trip.additional_receipt_bank_transfer_amount, 0) - tsr.tollgate_fee,
        IFNULL(trip.additional_receipt_bank_transfer_amount, 0)
      ),
      0
    ) AS bank_transfer_amount,
    IF(
      payment_method_token.type = 'EXIMBAY' 
      OR payment.created_at < '2020-09-02 15:00:00' 
      OR (
        payment_method.card_publisher = 'KB국민카드' AND (tsr.payment_extra_type is null OR tsr.payment_extra_type = 'RECHARGE')),
      if(
        tsr.payment_extra_type = 'TIP',
        tsr.service_fee,
        cast(tsr.service_fee - round(tsr.meter_amount * 0.1) AS INT64)
      ),
      cast(round((tsr.pay_amount - tsr.vcnc_contribution) * 0.0209) AS INT64)
    ) AS pg_amount,
    if(
      payment_method_token.type = 'EXIMBAY' 
      OR payment.created_at < '2020-09-02 15:00:00' 
      OR (
        payment_method.card_publisher = 'KB국민카드' 
        AND (tsr.payment_extra_type IS NULL OR tsr.payment_extra_type = 'RECHARGE')
      ),
      IF(
        tsr.payment_extra_type = 'TIP', tsr.service_fee, CAST(ROUND(tsr.service_fee - ROUND(tsr.meter_amount * 0.1), -1) AS INT64)
      ),
      CAST(ROUND((tsr.card_contribution) * 0.0209, -1) AS INT64)
    ) AS pg_amount_v2,
    (
      IF(
        payment_method_token.type = 'EXIMBAY' 
        OR payment.created_at < '2020-09-02 15:00:00' 
        OR (
          payment_method.card_publisher = 'KB국민카드' 
          AND (tsr.payment_extra_type IS NULL OR tsr.payment_extra_type = 'RECHARGE')
        ),
        IF(tsr.payment_extra_type = 'TIP', tsr.service_fee, CAST(tsr.service_fee - ROUND(tsr.meter_amount * 0.1) AS INT64)),
        0
      ) 
      + tsr.pay_amount 
      - tsr.service_fee 
    ) AS driver_amount,
    IF(tsr.payment_id IS NOT NULL, 'Y', 'N') AS pg_amount_exist,
    IF(tsr.payment_id IS NOT NULL, IFNULL(payment_transaction.amount, 0) - IFNULL(partial_refund.amount, 0), 0) AS payment_transaction_amount,
    IF(tsr.payment_id IS NOT NULL, IFNULL(payment_method.type, 'CARD'), NULL) AS payment_method_type,
    IF(tsr.payment_id IS NOT NULL, IFNULL(payment_method.toss_app_pay_type, ""), NULL) AS payment_method_toss_pay_type,
    IF(tsr.payment_id IS NOT NULL, IFNULL(payment_method_token.type, ""), NULL) AS payment_method_token_type,
    IF(tsr.payment_id IS NOT NULL, IFNULL(payment_method_token.publisher_name, ""), NULL) AS payment_method_card_publisher,
    IF(tsr.payment_id IS NOT NULL, IFNULL(payment_transaction.pg_approval_number, ""), NULL) AS pg_approval_number,
    IF(tsr.payment_id IS NOT NULL, IFNULL(payment_transaction.id, ""), NULL) AS payment_transaction_id
  FROM
    tada.tmoney_settlement_record AS tsr
    JOIN tada.trip on tsr.trip_id = trip.id
    LEFT JOIN tada.driver on tsr.driver_id = driver.id
    LEFT JOIN tada.driver_agency on driver.agency_id = driver_agency.id
    LEFT JOIN tada.vehicle on tsr.vehicle_id = vehicle.id
    LEFT JOIN (
      SELECT *
      FROM (SELECT *, COUNTIF(extra_type = 'RECHARGE') OVER (PARTITION BY trip_id) AS cnt FROM tada.payment)
      WHERE cnt = 0 OR extra_type IS NOT NULL
    ) payment on payment.trip_id = tsr.trip_id AND tsr.is_outstANDing_balance is false
    LEFT JOIN tada.payment_transaction on payment_transaction.payment_id = payment.id AND payment_transaction.status = 'COMPLETED' AND payment_transaction.type = 'PAY'
    LEFT JOIN tada.payment_transaction partial_refund on partial_refund.payment_id = payment.id AND partial_refund.status = 'COMPLETED' AND partial_refund.type = 'PARTIAL_REFUND'
    LEFT JOIN tada.payment_method on payment_method.id = payment_transaction.payment_method_id
    LEFT JOIN tada.payment_method_token on payment_method_token.id = payment_transaction.payment_method_token_id
  where
    tsr.state = 'COMPLETED' 
    AND driver.id is not null 
    AND if(tsr.payment_id is null, true, tsr.payment_id = payment.id) 
    AND
    (
      if(tsr.payment_id is null, true, ifnull(payment.extra_type, '') = ifnull(tsr.payment_extra_type, '')) 
      OR tsr.is_outstanding_balance
    ) 
)

SELECT
  tmoney_transaction_id,
  transacted_at_kr,
  service_fee_date,
  deposit_date,
  is_outstanding_balance,
  should_waive_service_fee,
  agency_name,
  payment_type,
  payment_reason,
  license_plate,
  driver_id,
  driver_name,
  trip_id,
  trip_b2x,
  trip_type,
  if(payment_reason = '운임', coupon_discount, 0) AS coupon_discount,
  if(payment_reason = '운임', biz_discount, 0) AS biz_discount,
  if(payment_type = '결제', pay_amount, -pay_amount) AS pay_amount,
  if(payment_type = '결제', meter_amount, -meter_amount) AS meter_amount,
  if(payment_type = '결제', tollgate_fee, -tollgate_fee) AS tollgate_fee,
  if(payment_type = '결제', card_contribution, -card_contribution) AS card_contribution,
  if(payment_type = '결제', vcnc_contribution, -vcnc_contribution) AS vcnc_contribution,
  if(payment_type = '결제', service_fee, -service_fee) AS service_fee,
  if(payment_type = '결제', bank_transfer_amount, -bank_transfer_amount) AS bank_transfer_amount,
  if(payment_type = '결제', pg_amount, -pg_amount) AS pg_amount,
  if(payment_type = '결제', pg_amount_v2, -pg_amount_v2) AS pg_amount_v2,
  if(payment_type = '결제', driver_amount, -driver_amount) AS driver_amount,
  if(payment_type = '결제', vcnc_amount, -vcnc_amount) AS vcnc_amount,
  if(payment_type = '결제', payment_transaction_amount, -payment_transaction_amount) AS payment_transaction_amount,
  pg_amount_exist,
  payment_method_type,
  payment_method_token_type,
  payment_method_card_publisher,
  if (payment_method_token_type = 'TOSS_APP' and payment_method_toss_pay_type = 'BANK', payment_transaction_id, pg_approval_number) AS pg_approval_number
FROM base