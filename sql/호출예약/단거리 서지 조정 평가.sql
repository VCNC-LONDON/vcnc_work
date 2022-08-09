WITH

reserve AS (
  SELECT
    id AS reservation_id,
    (CASE
      WHEN DATE(created_at, "Asia/Seoul") BETWEEN "2022-07-16" AND "2022-08-04" THEN "단거리 적용 후"
      WHEN DATE(created_at, "Asia/Seoul") BETWEEN "2022-06-25" AND "2022-07-14" THEN "단거리 적용 전"
    END) AS is_test,
    created_at AS reserve_at,
    expected_pick_up_at AS onboard_at,
    driver_id AS matched_driver_id,
    CAST(json_value(ex_data, "$.surgePercentage") AS INT64) AS surge,
    CAST(json_value(estimation, "$.totalFee") AS INT64) AS fee,
    CAST(json_value(estimation, "$.distanceMeters") AS INT64) AS distanceMeters,
  FROM `kr-co-vcnc-tada.tada.ride_reservation`
),

accept AS (
  SELECT
    id AS accept_id,
    ride_reservation_id,
    driver_id,
    created_at AS accept_at,
    cancelled_at AS accept_cancel_at
  FROM `kr-co-vcnc-tada.tada.ride_reservation_acceptance`
),

base AS (
  SELECT 
    reservation_id,
    is_test,
    DATE(reserve_at, "Asia/Seoul") AS reserve_date_kr,
    (CASE
      WHEN EXTRACT(DAYOFWEEK FROM DATETIME(onboard_at, "Asia/Seoul")) = 1 THEN "7_일요일"
      WHEN EXTRACT(DAYOFWEEK FROM DATETIME(onboard_at, "Asia/Seoul")) = 2 THEN "1_월요일"
      WHEN EXTRACT(DAYOFWEEK FROM DATETIME(onboard_at, "Asia/Seoul")) = 3 THEN "2_화요일"
      WHEN EXTRACT(DAYOFWEEK FROM DATETIME(onboard_at, "Asia/Seoul")) = 4 THEN "3_수요일"
      WHEN EXTRACT(DAYOFWEEK FROM DATETIME(onboard_at, "Asia/Seoul")) = 5 THEN "4_목요일"
      WHEN EXTRACT(DAYOFWEEK FROM DATETIME(onboard_at, "Asia/Seoul")) = 6 THEN "5_금요일"
      WHEN EXTRACT(DAYOFWEEK FROM DATETIME(onboard_at, "Asia/Seoul")) = 7 THEN "6_토요일"
    END) AS onboard_daynum,
    EXTRACT(HOUR FROM DATETIME(onboard_at, "Asia/Seoul")) AS onboard_hour,
    a.driver_id AS accept_driver_id,
    a.accept_id,
    DATETIME(accept_at, "Asia/Seoul") AS accept_dt,
    DATETIME(accept_cancel_at, "Asia/Seoul") AS accept_cancel_dt,
    surge,
    r.distanceMeters/1000 AS dist_km,
    (CASE
      WHEN distanceMeters < 3000 THEN 3000 
      WHEN distanceMeters < 4200 THEN 4200
      WHEN distanceMeters < 5400 THEN 5400
      WHEN distanceMeters < 6700 THEN 6700
      WHEN distanceMeters < 8200 THEN 8200
      WHEN distanceMeters < 10000 THEN 10000
      WHEN distanceMeters < 12300 THEN 12300
      WHEN distanceMeters < 15500 THEN 15500
      WHEN distanceMeters < 21300 THEN 21300
      WHEN distanceMeters >= 21300 THEN 21500
    END) AS dist_div,
    fee
  FROM reserve AS r
  LEFT JOIN accept AS a
  ON r.reservation_id = a.ride_reservation_id
  WHERE is_test is NOT NULL
)

SELECT
  is_test,
  onboard_daynum,
  onboard_hour,
  dist_div,
  surge,
  fee,
  COUNT(DISTINCT reservation_id ) AS reserve_cnt,
  COUNT(DISTINCT IF(accept_id IS NOT NULL, reservation_id, NULL)) AS accept_reserve_cnt,
FROM base
GROUP BY 1, 2, 3, 4, 5, 6