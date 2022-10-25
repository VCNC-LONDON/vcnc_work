## membership 시리즈와 payment 시리즈에 index나 partitioning 이 안되어 있어서 어차피 항상 full scan 해야한다.
# record로 쌓는거니까 적어도 적재하는 결과만큼은..파티셔닝, 클러스터링 해놓고 쓸 수 있게 하자..

SELECT
  DATE(renewal.created_at, 'Asia/Seoul') AS date_kr, -- 가입(갱신)일
  renewal.membership_id AS membership_id, -- 가입번호
  renewal.id AS renewal_id, -- 리뉴얼 아이디
  renewal.user_id AS user_id, -- 유저 ID
  product.price AS price, -- 상품 가격
  DATETIME_TRUNC(DATETIME(renewal.valid_after, 'Asia/Seoul'), SECOND) AS start_at_kr, -- 시작일시
  DATETIME_TRUNC(DATETIME(renewal.expires_at, 'Asia/Seoul'), SECOND) AS end_at_kr, -- 종료일시
  DATETIME_TRUNC(DATETIME(renewal.created_at, 'Asia/Seoul'), SECOND) AS created_at_kr, -- 가입일시
  DATETIME_TRUNC(DATETIME(renewal.refunded_at, 'Asia/Seoul'), SECOND) AS refunded_at_kr, -- 해지일시
  IF(tx.type = 'PAY', '결제', '환불') AS tx_type, -- 결제/환불 여부
  token.type AS pg_name, -- PG사
  token.publisher_name AS card_publisher_name, -- 카드사
  IF(tx.type = 'PAY', tx.pg_approval_number, original_tx.pg_approval_number) AS pg_approval_number, -- PG사 승인번호
  IF(
    tx.type = 'PAY',
    DATETIME_TRUNC(DATETIME(tx.pg_approved_at, 'Asia/Seoul'), SECOND),
    DATETIME_TRUNC(DATETIME(tx.pg_refunded_at, 'Asia/Seoul'), SECOND)
  ) AS transacted_at_kr, -- 결제/환불시각
  IF(tx.type = 'PAY', tx.amount, -tx.amount) AS tx_amount, -- 결제/환불금액
FROM
  `kr-co-vcnc-tada.tada.user_membership_renewal` AS renewal
  LEFT OUTER JOIN `kr-co-vcnc-tada.tada.user_membership` membership ON membership.id = renewal.membership_id
  LEFT OUTER JOIN `kr-co-vcnc-tada.tada.user_subscription_billing` billing ON billing.id = renewal.billing_id
  LEFT OUTER JOIN `kr-co-vcnc-tada.tada.user_subscription_product` product ON  product.identifier = renewal.product_identifier
  LEFT OUTER JOIN `kr-co-vcnc-tada.tada.payment` payment ON payment.user_subscription_billing_id = billing.id
  LEFT OUTER JOIN `kr-co-vcnc-tada.tada.payment_transaction` tx ON payment.id = tx.payment_id
  LEFT OUTER JOIN `kr-co-vcnc-tada.tada.payment_transaction` original_tx ON original_tx.id = tx.original_transaction_id
  LEFT OUTER JOIN `kr-co-vcnc-tada.tada.payment_method_token` token ON token.id = tx.payment_method_token_id
WHERE
  membership.source = 'VCNC'
  AND tx.status != 'FAILED'