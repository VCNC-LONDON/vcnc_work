WITH 

_surge AS (
    SELECT 
        DISTINCT IF(EXTRACT(dayofweek FROM created_at_kr) = 1, 7, EXTRACT(dayofweek FROM created_at_kr) - 1 ) AS weeknum,
        type,
        date_kr,
        FORMAT_TIMESTAMP("%H:%M:%S", TIMESTAMP_SECONDS(600 * DIV(UNIX_SECONDS(TIMESTAMP(created_at_kr)) + 300, 600))) AS created_dt_trunc,
        AVG(IFNULL(surge_percentage,100)) AS surge,
        COUNT(ride_id) AS call_cnt,
        COUNT(DISTINCT rider_id) AS unq_call_rider_cnt,
        -- SUM(receipt_total) AS fee,
        -- SUM(receipt_total - receipt_discount_amount) AS fee_except_discount
        -- FLOOR(distance_meters/1000) AS dist_km,
    FROM 
        `kr-co-vcnc-tada.tada_store.ride_base`
    JOIN 
        (SELECT id FROM `kr-co-vcnc-tada.tada.user` WHERE company IS NULL ) 
        ON id = rider_id
    WHERE
        date_kr BETWEEN DATE_SUB(CURRENT_DATE("Asia/Seoul"), interval 7 week) AND CURRENT_DATE("Asia/Seoul")
        AND type IN( "PREMIUM", "NXT" )
    GROUP BY 
        weeknum,
        type, 
        -- dist_km, 
        created_dt_trunc,
        date_kr
),

surge AS (
    SELECT
        weeknum,
        created_dt_trunc,
        IF(target_date = date_kr, NULL, CAST(DATE_DIFF(target_date, date_kr, day)/7 AS int64)  ) AS intvl,
        date_kr,
        surge,
    FROM 
        (SELECT *, FIRST_VALUE(date_kr) OVER (PARTITION BY weeknum ORDER BY date_kr DESC) AS target_date FROM _surge)
)

SELECT * FROM surge order by 1, 2, 3