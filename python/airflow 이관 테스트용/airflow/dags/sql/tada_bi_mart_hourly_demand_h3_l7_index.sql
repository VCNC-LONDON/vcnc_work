WITH h3_history AS (
  SELECT
    DISTINCT a.origin_h3_l7 AS origin_h3_l7_index,
    origin_region AS origin_h3_l7_region,
    b.index_name AS origin_h3_l7_name

  FROM
    tada_store.ride_base AS a
      LEFT JOIN tada_meta.h3_l7_name AS b
        ON a.origin_h3_l7 = b.h3_l7_index
  
  WHERE
    1=1
    AND date_kr >= date '2020-10-28'
    AND origin_region in ('서울', '부산', '성남', '경기')
),

h3_poly_base AS (
  SELECT
    *,
    ST_GEOGFROMTEXT(
      ST_ASTEXT(
        jslibs.h3.ST_H3_BOUNDARY(
          origin_h3_l7_index
        )
      )
    ) AS poly_h3_l7

  FROM
    h3_history
),

h3_dummy_area AS (
  SELECT
    DATE(timestamp_dummy) AS date_kr,
    TIME(
      EXTRACT(
        HOUR FROM timestamp_dummy
      ), 0, 0
    ) AS hour_kr,
    EXTRACT(HOUR FROM timestamp_dummy) as hour_num_kr,
    driver_type,
    a.*

  FROM h3_poly_base AS a
    CROSS JOIN
      UNNEST(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP(CONCAT("{target_date}", " 00:00:00")), TIMESTAMP(CONCAT("{target_date}", " 23:00:00")), INTERVAL 1 HOUR)) AS timestamp_dummy
    CROSS JOIN
      UNNEST(['NXT', 'LITE', 'PLUS']) AS driver_type
),

ride_base AS (
  SELECT
    date_kr,
    created_at_kr,
    TIME(
      EXTRACT(
        HOUR from created_at_kr
      ), 0, 0
    ) AS hour_kr,
    REPLACE(type, 'PREMIUM', 'PLUS') AS type,
    ride_id,
    status,
    origin_h3_l7 AS origin_h3_l7_index,
    is_valid_5_last

  FROM
    tada_store.ride_base
    
  WHERE
    1=1
    AND date_kr = "{target_date}"
),

h3_base_aggregated AS (
  SELECT
    date_kr,
    hour_kr,
    type AS driver_type,
    origin_h3_l7_index,
    COUNT(DISTINCT ride_id) AS total_call_cnt,
    COUNTIF(is_valid_5_last IS TRUE) AS valid_call_cnt,
    COUNTIF(status = 'DROPPED_OFF') AS dropoff_cnt

  FROM ride_base

  GROUP BY
    date_kr,
    hour_kr,
    driver_type,
    origin_h3_l7_index
)


SELECT
    a.date_kr,
    a.hour_kr,
    a.hour_num_kr,
    a.driver_type,
    a.origin_h3_l7_index,
    a.origin_h3_l7_region,
    a.origin_h3_l7_name,
    IFNULL(b.total_call_cnt, 0) AS total_call_cnt,
    IFNULL(b.valid_call_cnt, 0) AS valid_call_cnt,
    IFNULL(b.dropoff_cnt, 0) AS dropoff_cnt,
    a.poly_h3_l7

FROM h3_dummy_area AS a
  LEFT JOIN h3_base_aggregated AS b
    ON a.date_kr = b.date_kr
      AND a.hour_kr = b.hour_kr
      AND a.driver_type = b.driver_type
      AND a.origin_h3_l7_index = b.origin_h3_l7_index;