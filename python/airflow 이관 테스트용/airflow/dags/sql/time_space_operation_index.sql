
    WITH dummy_time_space_table AS (
      SELECT
        DATETIME(time, 'Asia/Seoul') AS time_interval_kr,
        h3_l7_index,
        index_name AS h3_l7_name
      FROM
        tada_meta.seoul_h3_l7_name CROSS JOIN UNNEST(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP(DATE_SUB("{target_date}", INTERVAL 1 DAY), 'Asia/Seoul'), 
        TIMESTAMP(DATE_ADD("{target_date}", INTERVAL 1 DAY), 'Asia/Seoul'), INTERVAL 30 MINUTE)) AS time
      )
    
    , _demand_base AS (
      SELECT
        tada_udf.datetime_unit_minute(created_at_kr, 30) AS time_interval_kr,
        tada_udf.geo_to_h3(origin_lng, origin_lat, 7) AS h3_l7_index,
        COUNTIF(is_pure_session AND is_valid_5_last AND session_type = 'NXT') AS X,
        COUNTIF(is_pure_session AND is_valid_5_last AND session_type = 'LITE') AS L,
        COUNTIF(is_pure_session AND is_valid_5_last AND session_type = 'PREMIUM') AS P,
        COUNTIF(is_valid_5_last AND session_type = 'NEAR_TAXI') AS N,
        COUNTIF(is_valid_5_last AND session_type = 'MIXED') AS M,
        COUNTIF(is_valid_5_last) AS T,
        COUNTIF(session_type = 'NXT' AND status = 'DROPPED_OFF') AS XX,
        COUNTIF(session_type = 'LITE' AND status = 'DROPPED_OFF') AS LL,
        COUNTIF(session_type = 'PREMIUM' AND status = 'DROPPED_OFF') AS PP,
        COUNTIF(session_type = 'NEAR_TAXI' AND type = 'NXT' AND status = 'DROPPED_OFF') AS NX,
        COUNTIF(session_type = 'NEAR_TAXI' AND type = 'LITE' AND status = 'DROPPED_OFF') AS NL,
        COUNTIF(session_type = 'NEAR_TAXI' AND type = 'PREMIUM' AND status = 'DROPPED_OFF') AS NP,
        COUNTIF(session_type = 'MIXED' AND type = 'NXT' AND status = 'DROPPED_OFF') AS MX,
        COUNTIF(session_type = 'MIXED' AND type = 'LITE' AND status = 'DROPPED_OFF') AS ML,
        COUNTIF(session_type = 'MIXED' AND type = 'PREMIUM' AND status = 'DROPPED_OFF') AS MP,
        COUNTIF(status = 'DROPPED_OFF') AS TT,
        COUNTIF(status = 'DROPPED_OFF' AND type = 'NXT') AS TX,
        COUNTIF(status = 'DROPPED_OFF' AND type = 'LITE') AS TL,
        COUNTIF(status = 'DROPPED_OFF' AND type = 'PREMIUM') AS TP,
        SUM(IF(type = 'NXT' AND status = 'DROPPED_OFF', IFNULL(surge_percentage, 100), null)) AS X_surge,
        SUM(IF(type = 'LITE' AND status = 'DROPPED_OFF', IFNULL(surge_percentage, 100), null)) AS L_surge,
        SUM(IF(type = 'PREMIUM' AND status = 'DROPPED_OFF', IFNULL(surge_percentage, 100), null)) AS P_surge,
      FROM
        tada_store.ride_base
      WHERE
        date_kr BETWEEN DATE_SUB("{target_date}", INTERVAL 1 DAY) AND DATE_ADD("{target_date}", INTERVAL 1 DAY) 
        AND origin_region = '서울'
      GROUP BY
        time_interval_kr, h3_l7_index
      )
    
    , demand_base AS (
      SELECT
        dummy_time_space_table.*,
        IFNULL(X, 0) AS X,
        IFNULL(L, 0) AS L,
        IFNULL(P, 0) AS P,
        IFNULL(N, 0) AS N,
        IFNULL(M, 0) AS M,
        IFNULL(T, 0) AS T,
        IFNULL(XX, 0) AS XX,
        IFNULL(LL, 0) AS LL,
        IFNULL(PP, 0) AS PP,
        IFNULL(NX, 0) AS NX,
        IFNULL(NL, 0) AS NL,
        IFNULL(NP, 0) AS NP,
        IFNULL(MX, 0) AS MX,
        IFNULL(ML, 0) AS ML,
        IFNULL(MP, 0) AS MP,
        IFNULL(TT, 0) AS TT,
        IFNULL(TX, 0) AS TX,
        IFNULL(TL, 0) AS TL,
        IFNULL(TP, 0) AS TP
      FROM
        dummy_time_space_table LEFT JOIN _demand_base
        USING(time_interval_kr, h3_l7_index)
      )
    
    , operation_weight AS (
      SELECT
        *,
        -- 하차완료율
        ROUND(IFNULL(SAFE_DIVIDE(XX, X), 0), 3) AS X_drr,
        ROUND(IFNULL(SAFE_DIVIDE(LL, L), 0), 3) AS L_drr,
        ROUND(IFNULL(SAFE_DIVIDE(PP, P), 0), 3) AS P_drr,
        ROUND(IFNULL(SAFE_DIVIDE(NX + NL + NP, N), 0), 3) AS N_drr,
        ROUND(IFNULL(SAFE_DIVIDE(MX + ML + MP, M), 0), 3) AS M_drr,
        ROUND(IFNULL(SAFE_DIVIDE(TT, T), 0), 3) AS T_drr,
        -- 운영 하차완료율 가중치
        IFNULL(SAFE_DIVIDE(XX, TX), 0) AS XX_drr_weight,
        IFNULL(SAFE_DIVIDE(NX, TX), 0) AS NX_drr_weight,
        IFNULL(SAFE_DIVIDE(MX, TX), 0) AS MX_drr_weight,
        IFNULL(SAFE_DIVIDE(LL, TL), 0) AS LL_drr_weight,
        IFNULL(SAFE_DIVIDE(NL, TL), 0) AS NL_drr_weight,
        IFNULL(SAFE_DIVIDE(ML, TL), 0) AS ML_drr_weight,
        IFNULL(SAFE_DIVIDE(PP, TP), 0) AS PP_drr_weight,
        IFNULL(SAFE_DIVIDE(NP, TP), 0) AS NP_drr_weight,
        IFNULL(SAFE_DIVIDE(MP, TP), 0) AS MP_drr_weight,
        -- 운영 수요 가중치
        CASE WHEN NX = 0 AND NL = 0 AND NP = 0 THEN 0.333 ELSE SAFE_DIVIDE(NX, (NX + NL + NP)) END AS NX_demand_weight,
        CASE WHEN NX = 0 AND NL = 0 AND NP = 0 THEN 0.333 ELSE SAFE_DIVIDE(NL, (NX + NL + NP)) END AS NL_demand_weight,
        CASE WHEN NX = 0 AND NL = 0 AND NP = 0 THEN 0.333 ELSE SAFE_DIVIDE(NP, (NX + NL + NP)) END AS NP_demand_weight,
        CASE WHEN MX = 0 AND ML = 0 AND MP = 0 THEN 0.333 ELSE SAFE_DIVIDE(MX, (MX + ML + MP)) END AS MX_demand_weight,
        CASE WHEN MX = 0 AND ML = 0 AND MP = 0 THEN 0.333 ELSE SAFE_DIVIDE(ML, (MX + ML + MP)) END AS ML_demand_weight,
        CASE WHEN MX = 0 AND ML = 0 AND MP = 0 THEN 0.333 ELSE SAFE_DIVIDE(MP, (MX + ML + MP)) END AS MP_demand_weight,
      FROM
        demand_base
      )
    
    , _operation_index AS (
      SELECT
        *,
        -- 운영 수요
        ROUND(X + (N * NX_demand_weight) + (M * MX_demand_weight)) AS X_op_demand,
        ROUND(L + (N * NL_demand_weight) + (M * ML_demand_weight)) AS L_op_demand,
        ROUND(P + (N * NP_demand_weight) + (M * MP_demand_weight)) AS P_op_demand,
        -- 운영 하차완료율
        ROUND((X_drr * XX_drr_weight) + (N_drr * NX_drr_weight) + (M_drr * MX_drr_weight), 3) AS X_op_drr,
        ROUND((L_drr * LL_drr_weight) + (N_drr * NL_drr_weight) + (M_drr * ML_drr_weight), 3) AS L_op_drr,
        ROUND((L_drr * PP_drr_weight) + (N_drr * NP_drr_weight) + (M_drr * MP_drr_weight), 3) AS P_op_drr,
      FROM
        operation_weight
      )
    
    , operation_index AS (
      SELECT
        * EXCEPT(XX_drr_weight, NX_drr_weight, MX_drr_weight, LL_drr_weight, NL_drr_weight, ML_drr_weight, PP_drr_weight, NP_drr_weight, MP_drr_weight, 
        NX_demand_weight, NL_demand_weight, NP_demand_weight, MX_demand_weight, ML_demand_weight, MP_demand_weight)
      FROM
        _operation_index
      )
    
    , _supply_base AS (
      SELECT
        tada_udf.datetime_unit_minute(DATETIME(gps_updated_at, 'Asia/Seoul'), 30) AS time_interval_kr,
        tada_udf.geo_to_h3(vehicle_location_lng, vehicle_location_lat, 7) AS h3_l7_index,
        ROUND(SAFE_DIVIDE(COUNTIF(driver_type = 'NXT' AND activity_status in ('RIDING', 'DISPATCHING')), 6)) AS X_supply,
        ROUND(SAFE_DIVIDE(COUNTIF(driver_type = 'LITE' AND activity_status in ('RIDING', 'DISPATCHING')), 6)) AS L_supply,
        ROUND(SAFE_DIVIDE(COUNTIF(driver_type = 'PREMIUM' AND activity_status in ('RIDING', 'DISPATCHING')), 6)) AS P_supply,
      FROM
        tada_prod_us.supply_snapshot
      WHERE
        date_kr BETWEEN DATE_SUB("{target_date}", INTERVAL 1 DAY) AND DATE_ADD("{target_date}", INTERVAL 1 DAY) 
        AND vehicle_location_address LIKE '서울%'
      GROUP BY
        time_interval_kr, h3_l7_index
      )
    
    , supply_base AS (
      SELECT 
        dummy_time_space_table.time_interval_kr,
        dummy_time_space_table.h3_l7_index,
        IFNULL(X_supply, 0) AS X_supply,
        IFNULL(L_supply, 0) AS L_supply,
        IFNULL(P_supply, 0) AS P_supply,
      FROM
        dummy_time_space_table LEFT JOIN _supply_base
        USING(time_interval_kr, h3_l7_index)
      )
    
    , result AS (
      SELECT
        DATE(time_interval_kr) AS date_kr,
        *,
        SPLIT(tada_udf.h3_to_geo(h3_l7_index), ',')[safe_offset(0)] AS h3_l7_lat,
        SPLIT(tada_udf.h3_to_geo(h3_l7_index), ',')[safe_offset(1)] AS h3_l7_lng,
      FROM
        operation_index LEFT JOIN supply_base
        USING(time_interval_kr, h3_l7_index)
      WHERE
        DATE(time_interval_kr) = "{target_date}"
      )
    
    SELECT
      *
    FROM
      result