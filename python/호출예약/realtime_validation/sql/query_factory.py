def get_raw_data_sql():
        sql = """
        WITH        

        reservation AS (
            SELECT 
                reservation_id, 
                driver_id, 
                ride_type,
                expected_pick_up_at, 
                prestarted_at,  
                start_lat, 
                start_lng, 
                origin_address AS start_address,
                origin_name AS start_name,
                estimate_total_fee, 
                ride_receipt_total + ride_receipt_discount_amount + ride_receipt_employee_discount_amount AS real_fee
            FROM `kr-co-vcnc-tada.tada_reservation.reservation_base` 
            WHERE prestarted_at IS NOT NULL  # 운행 성공 여부 등과 무관하게 prestart 가 없는 모든 호출예약은 효율화의 대상이 아니다.
            AND ride_type = "NXT"
            AND ride_date_kr BETWEEN "2022-02-01" AND "2022-02-28"
        ),      

        driver_location AS (
            SELECT 
                driver_id,  
                start_at,
                end_at, 
                lng_1 AS lng_seq_start, 
                lat_1 AS lat_seq_start, 
                lng_n AS lng_seq_end, 
                lat_n AS lat_seq_end, 
            FROM `kr-co-vcnc-tada.tada_ext.driver_activity_distance` 
            WHERE date_kr BETWEEN "2022-02-01" AND "2022-02-28"
            AND activity_status = "DISPATCHING" # prestart 는 실제로는 DISPATCHING 으로 전환되는 상태이다. 이 때, 이미 dispatching 중이거나 riding 중일 경우 나중에 찍힐 수 있다.
        ),      

        req_ride AS (
            SELECT 
                ride_id,
                first_call_service_type AS call_service_type ,
                session_final_status AS status,
                created_at_kr, 
                origin_lat, 
                origin_lng, 
                origin_address,
                destination_lat, 
                destination_lng,
                destination_address,
                IFNULL(estimation_original_max_cost, estimation_max_cost) AS estimate_ride_fee,
            FROM `kr-co-vcnc-tada.tada_store.ride_base` 
            WHERE date_kr BETWEEN "2022-02-01" AND "2022-02-28"
            AND session_start_at_kr = created_at_kr # 세션 중에서도 최초 라이드만 본다. 우리가 매칭 못시켜서 생기는 라이드가 존재하기 때문에 개별 수요로 보면 안됨
            AND call_service_type IN ("NXT", "NEAR_TAXI")
        ),      

        base AS (
            SELECT 
                r.* ,
                rr.*,
            FROM reservation AS r 
            LEFT JOIN req_ride AS rr 
            ON (CASE 
                    WHEN rr.call_service_type = "NEAR_TAXI" THEN rr.created_at_kr BETWEEN DATETIME(prestarted_at,"Asia/Seoul") AND DATETIME(expected_pick_up_at ,"Asia/Seoul")
                    WHEN rr.call_service_type != "NEAR_TAXI" THEN rr.call_service_type = r.ride_type AND rr.created_at_kr BETWEEN DATETIME(prestarted_at,"Asia/Seoul") AND DATETIME(expected_pick_up_at ,"Asia/Seoul")
                END)
        ),      

        aggr_all AS (
            SELECT 
                b.* ,
                ABS(DATETIME_DIFF(b.created_at_kr, DATETIME(dl.start_at,"Asia/Seoul"), SECOND)) AS diff_by_start,
                ABS(DATETIME_DIFF(b.created_at_kr, DATETIME(dl.end_at,"Asia/Seoul"), SECOND)) AS diff_by_end,
                IF(ABS(DATETIME_DIFF(b.created_at_kr, DATETIME(dl.start_at,"Asia/Seoul"), SECOND)) <= ABS(DATETIME_DIFF(b.created_at_kr, DATETIME(dl.end_at,"Asia/Seoul"), SECOND)), dl.start_at, dl.end_at) AS approxy_driver_location_logged_at, 
                IF(ABS(DATETIME_DIFF(b.created_at_kr, DATETIME(dl.start_at,"Asia/Seoul"), SECOND)) <= ABS(DATETIME_DIFF(b.created_at_kr, DATETIME(dl.end_at,"Asia/Seoul"), SECOND)), dl.lng_seq_start, dl.lng_seq_end) AS approxy_driver_lng, 
                IF(ABS(DATETIME_DIFF(b.created_at_kr, DATETIME(dl.start_at,"Asia/Seoul"), SECOND)) <= ABS(DATETIME_DIFF(b.created_at_kr, DATETIME(dl.end_at,"Asia/Seoul"), SECOND)), dl.lat_seq_start, dl.lat_seq_end) AS approxy_driver_lat, 
            FROM base AS b
            LEFT JOIN driver_location AS dl ON b.driver_id = dl.driver_id 
            QUALIFY ROW_NUMBER() OVER (PARTITION BY driver_id, reservation_id, ride_id ORDER BY IF(diff_by_start <= diff_by_end, diff_by_start, diff_by_end) ) = 1
            AND ST_DISTANCE(ST_GEOGPOINT(approxy_driver_lng,approxy_driver_lat), ST_GEOGPOINT(origin_lng, origin_lat)) < 6000 # 배차 로직상 지역1,2,3 이 3,4,6km 거리의 라이드만 배차 대상으로 본다. 일단 최대 조건을 보는거니 6km 로 잡는다.
        )       

        SELECT  
            reservation_id,
            driver_id,
            ride_type,
            expected_pick_up_at,
            prestarted_at,
            start_lat,
            start_lng,
            start_address,
            start_name,
            estimate_total_fee,
            real_fee,
            approxy_driver_location_logged_at,
            approxy_driver_lng,
            approxy_driver_lat,
            ride_id,
            call_service_type,
            status,
            TIMESTAMP(DATETIME_SUB(created_at_kr, INTERVAL 9 hour)) AS created_at,
            origin_lat,
            origin_lng,
            origin_address,
            destination_lat,
            destination_lng,
            ROW_NUMBER() OVER (PARTITION BY reservation_id ORDER BY created_at_kr) AS ride_rn,
            estimate_ride_fee,
        FROM aggr_all 
        WHERE approxy_driver_lat IS NOT NULL
        """

        return sql
