WITH dummy_table AS (
      SELECT
        TIME(time) AS time_interval_kr,
        h3_l7_index
      FROM
        tada_prod_us.seoul_h3_l7_region_class CROSS JOIN UNNEST(GENERATE_TIMESTAMP_ARRAY('2022-07-01 00:00:00', '2022-07-01 23:50:00', INTERVAL 10 MINUTE)) as time
      ),

    ride_est_base AS (
      SELECT
        TIME(tada_udf.datetime_unit_minute(DATETIME_TRUNC(DATETIME(TIMESTAMP_MILLIS(time_ms), 'Asia/Seoul'), SECOND), 10)) AS time_interval_kr,
        h3_l7_index,
        -- 호출뷰 진입 유저 수
        COUNT(DISTINCT user_id) AS estimation_view_usr_cnt
      FROM
        tada_prod_us.ride_estimations, UNNEST(estimations) AS estimation
        JOIN tada_prod_us.seoul_h3_l7_region_class
        ON tada_udf.geo_to_h3(estimation.origin_lng, estimation.origin_lat, 7) = seoul_h3_l7_region_class.h3_l7_index
      WHERE
        date_kr = "{target_date}"
        AND estimation.origin_address LIKE '서울%'
      GROUP BY
        time_interval_kr, h3_l7_index
      ),

    ride_est AS (
      SELECT
        dummy_table.time_interval_kr,
        CAST(CONCAT("{target_date}", ' ', dummy_table.time_interval_kr) AS DATETIME) AS date_time_kr,
        dummy_table.h3_l7_index,
        tada_udf.h3_to_geowkt(dummy_table.h3_l7_index) AS h3_l7_centroid,
        SPLIT(tada_udf.h3_to_geo(dummy_table.h3_l7_index), ',')[OFFSET(0)] AS h3_l7_centroid_lat,
        SPLIT(tada_udf.h3_to_geo(dummy_table.h3_l7_index), ',')[OFFSET(1)] AS h3_l7_centroid_lng,
        IFNULL(estimation_view_usr_cnt,0) AS call_view_usr,
      FROM  dummy_table
      LEFT JOIN ride_est_base
      ON dummy_table.time_interval_kr = ride_est_base.time_interval_kr
      AND dummy_table.h3_l7_index = ride_est_base.h3_l7_index
    ),

    # 호출뷰 진입 유저 수 (실시간 파이프라인 활용) 전날 값 으로 00:00 값 뽑아내기
    ride_est_noon_base AS (
      SELECT
        TIME(tada_udf.datetime_unit_minute(DATETIME_TRUNC(DATETIME(TIMESTAMP_MILLIS(time_ms), 'Asia/Seoul'), SECOND), 10)) AS time_interval_kr,
        h3_l7_index,
        COUNT(DISTINCT user_id) AS estimation_view_usr_cnt
      FROM
        tada_prod_us.ride_estimations, UNNEST(estimations) AS estimation
        JOIN tada_prod_us.seoul_h3_l7_region_class
        ON tada_udf.geo_to_h3(estimation.origin_lng, estimation.origin_lat, 7) = seoul_h3_l7_region_class.h3_l7_index
      WHERE
        date_kr = DATE_ADD("{target_date}", INTERVAL -1 DAY)
        AND estimation.origin_address LIKE '서울%'
      GROUP BY
        time_interval_kr, h3_l7_index
      ),

    ride_est_noon AS (
      SELECT
        dummy_table.time_interval_kr,
        CAST(CONCAT(DATE_ADD("{target_date}", INTERVAL -1 DAY) , ' ', dummy_table.time_interval_kr) AS DATETIME) AS date_time_kr,
        dummy_table.h3_l7_index,
        tada_udf.h3_to_geowkt(dummy_table.h3_l7_index) AS h3_l7_centroid,
        SPLIT(tada_udf.h3_to_geo(dummy_table.h3_l7_index), ',')[OFFSET(0)] AS h3_l7_centroid_lat,
        SPLIT(tada_udf.h3_to_geo(dummy_table.h3_l7_index), ',')[OFFSET(1)] AS h3_l7_centroid_lng,
        IFNULL(estimation_view_usr_cnt,0) AS call_view_usr
      FROM  dummy_table
      LEFT JOIN ride_est_noon_base
      ON dummy_table.time_interval_kr = ride_est_noon_base.time_interval_kr
      AND dummy_table.h3_l7_index = ride_est_noon_base.h3_l7_index
      WHERE dummy_table.time_interval_kr >= '23:30:00'
    ),

    union_data AS (
    SELECT *
    FROM ride_est

    UNION ALL

    SELECT *
    FROM ride_est_noon
    ),

    scaling_base_data AS(
      SELECT
        date(date_time_kr) AS date_kr,
        time_interval_kr,
        EXTRACT(HOUR FROM time_interval_kr) AS hour_kr,
        h3_l7_index,
        h3_l7_centroid_lat,
        h3_l7_centroid_lng,
        IFNULL(ROUND(SUM(call_view_usr) OVER(PARTITION BY h3_l7_index ORDER BY date_time_kr ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING),1),0)+1 AS call_view_usr_30min,
        ROUND(LOG(IFNULL(ROUND(SUM(call_view_usr) OVER(PARTITION BY h3_l7_index ORDER BY date_time_kr ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING),1),0)+1, 300),1) AS log_scaled_call_view_usr_30min
      FROM
        union_data
    ),

    scaled_data AS(
      SELECT
        datetime(date_kr, time_interval_kr) as date_time_kr,
        date_kr,
        time_interval_kr,
        h3_l7_index,
        h3_l7_centroid_lat,
        h3_l7_centroid_lng,
        log_scaled_call_view_usr_30min,
        CASE WHEN log_scaled_call_view_usr_30min <= 0 AND hour_kr IN (8,9,17,18,21,22,23,0,1) THEN 'peak_1'
        WHEN log_scaled_call_view_usr_30min <= 0.2 AND hour_kr IN (8,9,17,18,21,22,23,0,1) THEN 'peak_2'
        WHEN log_scaled_call_view_usr_30min <= 0.3 AND hour_kr IN (8,9,17,18,21,22,23,0,1) THEN 'peak_3'
        WHEN log_scaled_call_view_usr_30min <= 0.4 AND hour_kr IN (8,9,17,18,21,22,23,0,1) THEN 'peak_4'
        WHEN log_scaled_call_view_usr_30min <= 0.5 AND hour_kr IN (8,9,17,18,21,22,23,0,1) THEN 'peak_5'
        WHEN log_scaled_call_view_usr_30min <= 0.7 AND hour_kr IN (8,9,17,18,21,22,23,0,1) THEN 'peak_6'
        WHEN log_scaled_call_view_usr_30min > 0.7 AND hour_kr IN (8,9,17,18,21,22,23,0,1) THEN 'peak_7'
        WHEN log_scaled_call_view_usr_30min <= 0 AND hour_kr IN (2,3,4,5,6,7,10,11,12,13,14,15,16,19,20) THEN 'offpeak_1'
        WHEN log_scaled_call_view_usr_30min <= 0.2 AND hour_kr IN (2,3,4,5,6,7,10,11,12,13,14,15,16,19,20) THEN 'offpeak_2'
        WHEN log_scaled_call_view_usr_30min <= 0.4 AND hour_kr IN (2,3,4,5,6,7,10,11,12,13,14,15,16,19,20) THEN 'offpeak_3'
        WHEN log_scaled_call_view_usr_30min <= 0.5 AND hour_kr IN (2,3,4,5,6,7,10,11,12,13,14,15,16,19,20) THEN 'offpeak_4'
        WHEN log_scaled_call_view_usr_30min > 0.5  AND hour_kr IN (2,3,4,5,6,7,10,11,12,13,14,15,16,19,20) THEN 'offpeak_5'
        END AS class
    FROM scaling_base_data
    WHERE date_kr = "{target_date}"
    )

    SELECT
      *
    FROM
      scaled_data