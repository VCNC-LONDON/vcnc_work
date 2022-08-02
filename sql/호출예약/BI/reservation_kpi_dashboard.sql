WITH

date_dummy AS (
  SELECT
    date_kr
  FROM UNNEST(GENERATE_DATE_ARRAY("2022-01-19", "2022-07-31", INTERVAL 1 DAY)) AS date_kr
),

reserve_raw AS (
  SELECT
    DATE(expected_pick_up_at, "Asia/Seoul") AS ride_date_kr,
    EXTRACT(HOUR FROM DATETIME(expected_pick_up_at, "Asia/Seoul")) AS onboard_hour,
    (CASE 
      WHEN IFNULL(JSON_VALUE(rr.ex_data, "$.originAddressDetail.siDo"),"") IN ("서울") THEN CONCAT(IFNULL(JSON_VALUE(rr.ex_data, "$.originAddressDetail.siDo"),""),"특별시") 
      WHEN IFNULL(JSON_VALUE(rr.ex_data, "$.originAddressDetail.siDo"),"") IN ("세종") THEN CONCAT(IFNULL(JSON_VALUE(rr.ex_data, "$.originAddressDetail.siDo"),""),"특별자치시") 
      WHEN IFNULL(JSON_VALUE(rr.ex_data, "$.originAddressDetail.siDo"),"") IN ("경기","강원") THEN CONCAT(IFNULL(JSON_VALUE(rr.ex_data, "$.originAddressDetail.siDo"),""),"도") 
      WHEN IFNULL(JSON_VALUE(rr.ex_data, "$.originAddressDetail.siDo"),"") IN ("대전", "부산", "대구", "인천", "광주", "울산") THEN CONCAT(IFNULL(JSON_VALUE(rr.ex_data, "$.originAddressDetail.siDo"),""),"광역시") 
      WHEN IFNULL(JSON_VALUE(rr.ex_data, "$.originAddressDetail.siDo"),"") = "전북" THEN "전라북도"
      WHEN IFNULL(JSON_VALUE(rr.ex_data, "$.originAddressDetail.siDo"),"") = "전남" THEN "전라남도"
      WHEN IFNULL(JSON_VALUE(rr.ex_data, "$.originAddressDetail.siDo"),"") = "경북" THEN "경상북도"
      WHEN IFNULL(JSON_VALUE(rr.ex_data, "$.originAddressDetail.siDo"),"") = "경남" THEN "경상남도"
      WHEN IFNULL(JSON_VALUE(rr.ex_data, "$.originAddressDetail.siDo"),"") = "충북" THEN "충청북도"
      WHEN IFNULL(JSON_VALUE(rr.ex_data, "$.originAddressDetail.siDo"),"") = "충남" THEN "충청남도"
      ELSE IFNULL(JSON_VALUE(rr.ex_data, "$.originAddressDetail.siDo"),"")
    END) AS origin_sido,
    IFNULL(JSON_VALUE(rr.ex_data, "$.originAddressDetail.siGunGu"),"") AS origin_sgg,
    (CASE 
      WHEN IFNULL(JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.siDo"),"") IN ("서울") THEN CONCAT(IFNULL(JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.siDo"),""),"특별시") 
      WHEN IFNULL(JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.siDo"),"") IN ("세종") THEN CONCAT(IFNULL(JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.siDo"),""),"특별자치시") 
      WHEN IFNULL(JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.siDo"),"") IN ("경기","강원") THEN CONCAT(IFNULL(JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.siDo"),""),"도") 
      WHEN IFNULL(JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.siDo"),"") IN ("대전", "부산", "대구", "인천", "광주", "울산") THEN CONCAT(IFNULL(JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.siDo"),""),"광역시") 
      WHEN IFNULL(JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.siDo"),"") = "전북" THEN "전라북도"
      WHEN IFNULL(JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.siDo"),"") = "전남" THEN "전라남도"
      WHEN IFNULL(JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.siDo"),"") = "경북" THEN "경상북도"
      WHEN IFNULL(JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.siDo"),"") = "경남" THEN "경상남도"
      WHEN IFNULL(JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.siDo"),"") = "충북" THEN "충청북도"
      WHEN IFNULL(JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.siDo"),"") = "충남" THEN "충청남도"
      ELSE IFNULL(JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.siDo"),"")
    END) AS desti_sido,
    IFNULL(JSON_VALUE(rr.ex_data, "$.destinationAddressDetail.siGunGu"),"") AS desti_sgg,
    (CASE 
      WHEN TIMESTAMP_DIFF(expected_pick_up_at, rr.created_at, MINUTE)/60 <= 1 THEN 1
      WHEN TIMESTAMP_DIFF(expected_pick_up_at, rr.created_at, MINUTE)/60 <= 3 THEN 3
      WHEN TIMESTAMP_DIFF(expected_pick_up_at, rr.created_at, MINUTE)/60 <= 24 THEN FLOOR(TIMESTAMP_DIFF(expected_pick_up_at, rr.created_at, MINUTE)/60)
      WHEN TIMESTAMP_DIFF(expected_pick_up_at, rr.created_at, MINUTE)/60 <= 48 THEN 48
      WHEN TIMESTAMP_DIFF(expected_pick_up_at, rr.created_at, MINUTE)/60 <= 72 THEN 72
      WHEN TIMESTAMP_DIFF(expected_pick_up_at, rr.created_at, MINUTE)/60 <= 144 THEN 144
      WHEN TIMESTAMP_DIFF(expected_pick_up_at, rr.created_at, MINUTE)/60 <= 216 THEN 216
      WHEN TIMESTAMP_DIFF(expected_pick_up_at, rr.created_at, MINUTE)/60 <= 336 THEN 336
    END) AS reserve_lt_min_trunc,
    CAST(JSON_VALUE(rr.estimation, "$.surgePercentage") AS int64) AS surge,
    rr.id AS reserve_id,
    IF(COUNT(DISTINCT rra.id) OVER (PARTITION BY rr.id) > 0, rr.id, NULL) AS accept_at_least_once_reserve_id,
    IF(rr.driver_id IS NOT NULL, rr.id, NULL) AS matched_reserve_id
  FROM `kr-co-vcnc-tada.tada.ride_reservation` AS rr
  LEFT JOIN `kr-co-vcnc-tada.tada.ride_reservation_acceptance` AS rra ON rr.id = rra.ride_reservation_id
  WHERE DATE(expected_pick_up_at, "Asia/Seoul") <= "2022-07-31"
),

reserve AS (
  SELECT
    ride_date_kr,
    onboard_hour,
    origin_sido,
    origin_sgg,
    desti_sido,
    desti_sgg,
    reserve_lt_min_trunc,
    surge,
    COUNT(DISTINCT reserve_id) AS reserve_cnt,
    COUNT(DISTINCT accept_at_least_once_reserve_id) AS accept_reserve_cnt,
    COUNT(DISTINCT matched_reserve_id) AS matched_reserve_cnt,
  FROM reserve_raw
  GROUP BY ride_date_kr,onboard_hour,origin_sido,origin_sgg,desti_sido,desti_sgg,reserve_lt_min_trunc,surge
),

# 실시간/호출예약 시간당 매출 구하기용
-- driver_ban_record AS (
--   SELECT
--     DISTINCT driver_id,
--   FROM `kr-co-vcnc-tada.tada.driver_ban_record`
--   -- WHERE start_comment = "어드민에서 요청한 호출예약 차단입니다." 
--   WHERE type = "RIDE_RESERVATION" # 적어도 이 사유로 밴이 한번은 됐어야 "공급이 될 수 있는 파트너"로 볼 수 있다.
-- ),

-- ride AS (
--   SELECT
--     date_kr,
--     ride_id,
--     status,
--     is_reservation,
--     driver_id,
--     created_at_kr,
--     accepted_at_kr,
--     picked_up_at_kr,
--     dropped_off_at_kr,
--     IFNULL(receipt_total,0) AS rev,
--     IFNULL(credit_withdrawal_amount,0) AS credit, 
--     IFNULL(receipt_discount_amount,0) AS discount, 
--     IFNULL(receipt_employee_discount_amount,0) AS emp_discount
--   FROM `kr-co-vcnc-tada.tada_store.ride_base`
--   WHERE status IN ("DROPPED_OFF", "CANCELED") # 호출예약은 라이드 생기면 취소해도 매출이 생긴다
--   AND type IN ("NXT", "PREMIUM")
--   AND date_kr >= "2022-01-19"
-- ),

-- reserve AS (
--   SELECT
--     DATE(started_at, "Asia/Seoul") AS ride_date_kr,
--     driver_id,
--     DATETIME(prestarted_at, "Asia/Seoul") AS prestart_dt,
--     DATETIME(started_at, "Asia/Seoul") AS start_dt,
--     ride_id,
--     rev + credit + discount + emp_discount AS marchandise,
--     rev AS rev,
--     credit + discount + emp_discount AS discount
--   FROM `kr-co-vcnc-tada.tada.ride_reservation` AS reserve
--   JOIN (SELECT * FROM ride WHERE is_reservation) USING(ride_id, driver_id)
-- ),

# 이러면 로우수가 너무 많아져서 문제가 된다. 고민해볼 것
dummy_pp AS (
  SELECT
    date_dummy.date_kr,
    ss.origin_sido AS origin_sido,
    ss.origin_sgg AS origin_sgg,
    ss2.origin_sido AS desti_sido,
    ss2.origin_sgg AS desti_sgg,
    ob AS hour,
    rlh.reserve_lt_min_trunc AS reserve_lt_min_history
  FROM date_dummy
  CROSS JOIN (SELECT DISTINCT origin_sido, origin_sgg FROM reserve_raw UNION ALL SELECT DISTINCT desti_sido, desti_sgg FROM reserve_raw ) AS ss
  CROSS JOIN (SELECT DISTINCT origin_sido, origin_sgg FROM reserve_raw UNION ALL SELECT DISTINCT desti_sido, desti_sgg FROM reserve_raw ) AS ss2
  CROSS JOIN UNNEST(GENERATE_ARRAY(0,23)) AS ob
  CROSS JOIN (SELECT DISTINCT reserve_lt_min_trunc FROM reserve_raw) AS rlh
)

SELECT
  dp.* ,
  r.surge,
  IFNULL(r.reserve_cnt, 0) AS reserve_cnt,
  IFNULL(r.accept_reserve_cnt, 0) AS accept_reserve_cnt,
  IFNULL(r.matched_reserve_cnt, 0) AS matched_reserve_cnt
FROM dummy_pp AS dp
LEFT JOIN reserve AS r
ON dp.date_kr = r.ride_date_kr
AND dp.origin_sido = r.origin_sido
AND dp.origin_sgg = r.origin_sgg
AND dp.desti_sido = r.desti_sido
AND dp.desti_sgg = r.desti_sgg
AND dp.hour =r.onboard_hour
AND dp.reserve_lt_min_history = r.reserve_lt_min_trunc