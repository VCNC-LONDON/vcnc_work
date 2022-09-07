WITH

dummy AS (
  SELECT
    DATE "{target_date}" AS date_kr,
    type
  FROM UNNEST(["NXT","PREMIUM"]) AS type
),

reservation_by_reserveday AS (
  SELECT
    DATE(created_at, "Asia/Seoul") AS date_kr,
    ride_type,
    COUNT(id) AS reservation_cnt_by_reserve_day,
    COUNT(DISTINCT reservation_group_id) AS reservation_bulk_cnt_by_reserve_day,
    COUNT(DISTINCT user_id) AS reserve_user_cnt_by_reserve_day
  FROM `kr-co-vcnc-tada.tada.ride_reservation`
  WHERE DATE(created_at, "Asia/Seoul") = "{target_date}"
  GROUP BY date_kr, ride_type
),

reservation_by_dday AS (
  WITH 
  base AS (
    SELECT
      DATE(expected_pick_up_at, "Asia/Seoul") AS date_kr,
      rr.id AS reservation_id,
      rr.ride_type,
      ra.accept_cnt,
      rr.ride_id,
      rr.driver_id,
      rr.accepted_at,
      rr.cancelled_at,
      rr.status AS reservation_status,
      r.status AS ride_status,
      rr.cancellation_cause AS reservation_cancel_cause,
      r.cancellation_cause AS ride_cancel_cause,
      IFNULL(r.receipt_total,0) AS marchandise,
      IFNULL(r.receipt_extra,0) AS extra,
      IFNULL(r.receipt_discount_amount,0) AS coupon_discount,
      IFNULL(r.receipt_employee_discount_amount,0) AS employee_discount,
      IFNULL(r.credit_withdrawal_amount,0) AS credit_discount
    FROM `kr-co-vcnc-tada.tada.ride_reservation` AS rr
    LEFT JOIN (SELECT ride_reservation_id, COUNT(id) AS accept_cnt FROM `kr-co-vcnc-tada.tada.ride_reservation_acceptance` GROUP BY ride_reservation_id) AS ra ON rr.id = ra.ride_reservation_id
    LEFT JOIN (SELECT * FROM `kr-co-vcnc-tada.tada_store.ride_base` WHERE date_kr BETWEEN "{target_date}" AND "{target_date}" + 1) AS r ON rr.ride_id = r.ride_id 
    WHERE DATE(expected_pick_up_at, "Asia/Seoul") = "{target_date}"
  )

  SELECT
    date_kr,
    ride_type,
    COUNT(DISTINCT reservation_id) AS reservation_cnt_by_ride_day,
    COUNT(DISTINCT IF(accept_cnt >0, reservation_id, NULL)) AS accept_reservation_cnt_by_ride_day,
    COUNT(DISTINCT IF(driver_id IS NOT NULL, reservation_id, NULL)) AS matched_reservation_cnt_by_ride_day,
    COUNT(DISTINCT IF(ride_id IS NOT NULL AND ride_status = "DROPPED_OFF", reservation_id, NULL)) AS drop_reservation_cnt_by_ride_day,
    COUNT(DISTINCT IF(reservation_cancel_cause = "DISPATCH_TIMEOUT", reservation_id, NULL)) AS not_match_reserve_cnt_ride_day,
    COUNT(DISTINCT IF(reservation_status = "CANCELLED" AND reservation_cancel_cause != "DISPATCH_TIMEOUT", reservation_id, NULL)) AS cancel_reserve_cnt_ride_day,
    COUNT(DISTINCT IF(reservation_cancel_cause = "RIDER_CANCELLED" , reservation_id, NULL)) AS rider_cancel_reserve_cnt_ride_day,
    COUNT(DISTINCT IF(reservation_cancel_cause = "RIDER_CANCELLED" AND (accepted_at IS NULL OR accepted_at > cancelled_at), reservation_id, NULL)) AS rider_cancel_before_accept_reserve_cnt_ride_day,
    COUNT(DISTINCT IF(reservation_cancel_cause = "RIDER_CANCELLED" AND accepted_at <= cancelled_at, reservation_id, NULL)) AS rider_cancel_after_accept_reserve_cnt_ride_day,
    COUNT(DISTINCT IF(reservation_cancel_cause = "DRIVER_CANCEL_AFTER_EXPIRY", reservation_id, NULL)) AS driver_cancel_reserve_cnt_ride_day,
    COUNT(DISTINCT IF(reservation_cancel_cause = "ADMIN_CANCELLED", reservation_id, NULL)) AS admin_cancel_reserve_cnt_ride_day,
    SUM(marchandise + extra + coupon_discount + employee_discount + credit_discount) AS revenue,
    SUM(marchandise + extra) AS net_revenue,
    SUM(coupon_discount + employee_discount + credit_discount) AS discount,
  FROM base
  GROUP BY date_kr, ride_type
),

work_revenue AS (
  WITH
  reservation AS (
    SELECT
      DATE(expected_pick_up_at, "Asia/Seoul") AS date_kr,
      rr.id AS reservation_id,
      rr.driver_id,
      rr.ride_type,
      DATETIME(TIMESTAMP_ADD(rr.expected_pick_up_at,INTERVAL - 2 HOUR),"Asia/Seoul") AS reserve_logic_start_at_kr,
      DATETIME(IFNULL(rr.prestarted_at,rr.started_at),"Asia/Seoul") AS reserve_start_at_kr,
      r.dropped_off_at_kr,
      rr.ride_id,
    FROM `kr-co-vcnc-tada.tada.ride_reservation` AS rr
    LEFT JOIN `kr-co-vcnc-tada.tada_store.ride_base` AS r ON rr.ride_id = r.ride_id 
    WHERE DATE(expected_pick_up_at, "Asia/Seoul") = "{target_date}"
    AND date_kr BETWEEN "{target_date}" AND "{target_date}" + 1
  ),

  revenue AS (
    SELECT
      r.ride_id,
      r.status AS ride_status,
      IFNULL(r.receipt_total,0) AS marchandise,
      IFNULL(r.receipt_extra,0) AS extra,
      IFNULL(r.receipt_discount_amount,0) AS coupon_discount,
      IFNULL(r.receipt_employee_discount_amount,0) AS employee_discount,
      IFNULL(r.credit_withdrawal_amount,0) AS credit_discount
    FROM `kr-co-vcnc-tada.tada_store.ride_base` AS r
    WHERE date_kr BETWEEN "{target_date}" AND "{target_date}" + 1
  ),

  driver_activity AS (
    SELECT
      DISTINCT da.date_kr,
      da.driver_id,
      d.type AS driver_type,
      DATETIME(IF(da.activity_status = "RIDING" ,FIRST_VALUE(da.start_at) OVER (PARTITION BY ride_id ORDER BY start_at),  da.start_at), 'Asia/Seoul') activity_start_dt,
      DATETIME(IF(da.activity_status = "RIDING", FIRST_VALUE(da.end_at) OVER (PARTITION BY ride_id ORDER BY end_at DESC)  , da.end_at), 'Asia/Seoul') activity_end_dt,
      da.activity_status,
      da.ride_id,
    FROM `kr-co-vcnc-tada.tada.driver_activity` AS da
    JOIN (SELECT id, type FROM `kr-co-vcnc-tada.tada.driver` WHERE type IN ("NXT", "PREMIUM")) AS d
    ON da.driver_id = d.id
    WHERE da.date_kr BETWEEN "{target_date}"-1 AND "{target_date}" + 1
    AND SUBSTR(da.driver_id, 1, 3) NOT IN ('GIG', 'DVC') 
  ),

  activity_base AS (
    SELECT
      r.date_kr,
      r.reservation_id,
      r.ride_type,
      r.ride_id,
      r.driver_id,
      da.driver_type,
      r.reserve_logic_start_at_kr,
      r.reserve_start_at_kr,
      r.dropped_off_at_kr,
      da.activity_start_dt,
      da.activity_end_dt,
      da.activity_status,
      da.ride_id AS activity_ride_id,
      (CASE
        WHEN 
          reserve_logic_start_at_kr >= activity_start_dt 
          AND activity_status != "RIDING" 
        THEN true
        WHEN 
          activity_start_dt >= reserve_logic_start_at_kr 
          AND activity_end_dt BETWEEN reserve_logic_start_at_kr AND dropped_off_at_kr 
        THEN true
        ELSE false
      END) AS logical_trig,
      (CASE
        WHEN 
          (activity_start_dt >= reserve_start_at_kr AND activity_start_dt < dropped_off_at_kr) 
          OR activity_end_dt BETWEEN reserve_start_at_kr AND dropped_off_at_kr 
        THEN true
        ELSE false
      END) AS reserve_trig,
    FROM reservation AS r
    LEFT JOIN driver_activity AS da 
    ON r.driver_id = da.driver_id 
    WHERE activity_start_dt BETWEEN reserve_logic_start_at_kr AND dropped_off_at_kr
    OR activity_end_dt BETWEEN reserve_logic_start_at_kr AND dropped_off_at_kr 
  ),

  logical_activity AS (
    WITH

    base AS (
      SELECT
        date_kr,
        reservation_id,
        activity_status,
        driver_type,
        IF(activity_start_dt <= reserve_logic_start_at_kr, DATETIME_DIFF(activity_end_dt, reserve_logic_start_at_kr,  SECOND), DATETIME_DIFF(activity_end_dt, activity_start_dt, SECOND))/60 AS activity_duration,
        ab.ride_id,
        activity_ride_id,
        ride_status,
        SUM(marchandise + extra + coupon_discount + employee_discount + credit_discount) AS revenue,
        SUM(marchandise + extra) AS net_revenue,
        SUM(coupon_discount + employee_discount + credit_discount) AS discount,
      FROM activity_base AS ab
      LEFT JOIN revenue AS r ON ab.activity_ride_id = r.ride_id
      WHERE logical_trig
      GROUP BY 
        date_kr, reservation_id, activity_status, driver_type, activity_duration, ride_id, activity_ride_id, ride_status
    ),

    aggr AS (
      SELECT
        date_kr,
        reservation_id,
        driver_type,
        COUNT(IF(activity_ride_id != ride_id AND activity_ride_id IS NOT NULL, activity_ride_id, NULL)) AS  dispatch_realtime_ride_cnt,
        COUNT(IF(activity_ride_id != ride_id AND activity_ride_id IS NOT NULL AND ride_status = "DROPPED_OFF",  activity_ride_id, NULL)) AS drop_realtime_ride_cnt,
        SUM(IF(activity_status IN ("RIDING", "DISPATCHING"), activity_duration, 0)) AS working_min,
        SUM(IF(activity_status IN ("RIDING", "DISPATCHING") AND activity_ride_id != ride_id, activity_duration, 0)) AS  working_realtime_min,
        SUM(IFNULL(revenue,0)) AS rev,
        SUM(IFNULL(net_revenue,0)) AS net_revenue,
        SUM(IFNULL(discount,0)) AS discount,
      FROM base
      GROUP BY date_kr, reservation_id, driver_type
    )

    SELECT
      date_kr,
      driver_type,
      AVG(dispatch_realtime_ride_cnt) AS avg_dispatch_realtime_ride_cnt,
      AVG(drop_realtime_ride_cnt) AS avg_drop_realtime_ride_cnt,
      SAFE_DIVIDE(SUM(rev),SUM(working_min)/60) AS rev_per_working_hour,
    FROM aggr
    GROUP BY date_kr, driver_type
  ),

  standard_activity AS (
    WITH

    base AS (
      SELECT
        date_kr,
        reservation_id,
        driver_type,
        activity_status,
        IF(activity_start_dt <= reserve_start_at_kr, DATETIME_DIFF(activity_end_dt, reserve_start_at_kr, SECOND),   DATETIME_DIFF(activity_end_dt, activity_start_dt, SECOND))/60 AS activity_duration,
        ab.ride_id,
        activity_ride_id,
        ride_status,
        SUM(marchandise + extra + coupon_discount + employee_discount + credit_discount) AS revenue,
        SUM(marchandise + extra) AS net_revenue,
        SUM(coupon_discount + employee_discount + credit_discount) AS discount,
      FROM activity_base AS ab
      LEFT JOIN revenue AS r ON ab.activity_ride_id = r.ride_id
      WHERE reserve_trig
      GROUP BY 
        date_kr, reservation_id, driver_type, activity_status, activity_duration, ride_id,activity_ride_id, ride_status
    ),

    aggr AS (
      SELECT
        date_kr,
        reservation_id,
        driver_type,
        COUNT(IF(activity_ride_id != ride_id AND activity_ride_id IS NOT NULL, activity_ride_id, NULL)) AS  dispatch_realtime_ride_cnt,
        COUNT(IF(activity_ride_id != ride_id AND activity_ride_id IS NOT NULL AND ride_status = "DROPPED_OFF",  activity_ride_id, NULL)) AS drop_realtime_ride_cnt,
        SUM(IF(activity_status IN ("RIDING", "DISPATCHING"), activity_duration, 0)) AS working_min,
        SUM(IF(activity_status IN ("RIDING", "DISPATCHING") AND activity_ride_id != ride_id, activity_duration, 0)) AS  working_realtime_min,
        SUM(IFNULL(revenue,0)) AS rev,
        SUM(IFNULL(net_revenue,0)) AS net_revenue,
        SUM(IFNULL(discount,0)) AS discount,
      FROM base
      GROUP BY date_kr, reservation_id, driver_type
    )

    SELECT
      date_kr,
      driver_type,
      AVG(dispatch_realtime_ride_cnt) AS avg_dispatch_realtime_ride_cnt,
      AVG(drop_realtime_ride_cnt) AS avg_drop_realtime_ride_cnt,
      SAFE_DIVIDE(SUM(rev),SUM(working_min)/60) AS rev_per_working_hour,
    FROM aggr
    GROUP BY date_kr, driver_type
  )  

  SELECT
    d.date_kr,
    d.type,
    la.avg_dispatch_realtime_ride_cnt AS avg_realtime_dispatch_cnt_in_reserve_working_by_dispatch_logic,
    la.avg_drop_realtime_ride_cnt AS avg_realtime_drop_cnt_in_reserve_working_by_dispatch_logic,
    la.rev_per_working_hour AS rev_per_working_hour_by_dispatch_logic,
    sa.avg_dispatch_realtime_ride_cnt AS avg_realtime_dispatch_cnt_in_reserve_working_by_std,
    sa.avg_drop_realtime_ride_cnt AS avg_realtime_drop_cnt_in_reserve_working_by_std,
    sa.rev_per_working_hour AS rev_per_working_hour_by_std
  FROM dummy AS d
  LEFT JOIN logical_activity AS la ON d.date_kr = la.date_kr AND d.type = la.driver_type
  LEFT JOIN standard_activity AS sa ON d.date_kr = sa.date_kr AND d.type = sa.driver_type
),

work_driver AS (
  WITH
  driver_activity AS (
    SELECT
      DISTINCT da.date_kr,
      da.driver_id,
      d.type AS driver_type,
      TIMESTAMP_DIFF(da.end_at, da.start_at, SECOND)/60 AS status_duration,
      da.activity_status,
      da.ride_id,
    FROM `kr-co-vcnc-tada.tada.driver_activity` AS da
    JOIN (SELECT id, type FROM `kr-co-vcnc-tada.tada.driver` WHERE type IN ("NXT", "PREMIUM")) AS d
    ON da.driver_id = d.id
    WHERE da.date_kr = "{target_date}"
    AND SUBSTR(da.driver_id, 1, 3) NOT IN ('GIG', 'DVC') 
  ),

  work_driver AS (
    SELECT
      DISTINCT date_kr,
      driver_type,
      driver_id
    FROM driver_activity
    GROUP BY date_kr, driver_id, driver_type
    HAVING SUM(IF(activity_status NOT IN ("IDLE", "OFF"), status_duration,0)) >= 60
  ),

  ride AS (
    SELECT
      DATE(created_at_kr) AS date_kr_s,
      driver_id,
      COUNT(DISTINCT ride_id) AS drop_ride_cnt,
      COUNT(DISTINCT IF(is_reservation, ride_id, NULL)) AS drop_reservation_ride_cnt,
    FROM `kr-co-vcnc-tada.tada_store.ride_base` AS rb
    WHERE rb.date_kr BETWEEN "{target_date}" AND "{target_date}" + 1
    AND status = "DROPPED_OFF"
    GROUP BY date_kr_s, driver_id
    HAVING date_kr_s = "{target_date}"
  ),

  aggr AS (
    SELECT 
      wd.date_kr,
      wd.driver_type,
      wd.driver_id,
      IFNULL(r.drop_ride_cnt,0) AS drop_ride_cnt,
      IFNULL(r.drop_reservation_ride_cnt,0) AS drop_reservation_ride_cnt
    FROM work_driver AS wd
    LEFT JOIN ride AS r
    ON wd.driver_id = r.driver_id
  )

  SELECT 
    date_kr,
    driver_type AS type,
    COUNT(DISTINCT driver_id) AS work_driver_cnt,
    COUNT(DISTINCT IF(drop_ride_cnt > 0, driver_id, NULL)) AS drop_driver_cnt,
    COUNT(DISTINCT IF(drop_reservation_ride_cnt > 0, driver_id, NULL)) AS reserve_work_driver_cnt,
    SUM(drop_ride_cnt) AS work_driver_drop_ride_cnt,
    SUM(drop_reservation_ride_cnt) AS work_driver_drop_reservation_ride_cnt,
    AVG(drop_reservation_ride_cnt) AS avg_reserve_work_ratio
  FROM aggr
  GROUP BY date_kr, type
)

SELECT 
  d.date_kr,
  d.type,
  rbr.reservation_cnt_by_reserve_day,
  rbr.reservation_bulk_cnt_by_reserve_day,
  rbr.reserve_user_cnt_by_reserve_day,
  rbd.reservation_cnt_by_ride_day,
  rbd.accept_reservation_cnt_by_ride_day,
  rbd.matched_reservation_cnt_by_ride_day,
  rbd.drop_reservation_cnt_by_ride_day,
  rbd.not_match_reserve_cnt_ride_day,
  rbd.cancel_reserve_cnt_ride_day,
  rbd.rider_cancel_reserve_cnt_ride_day,
  rbd.rider_cancel_before_accept_reserve_cnt_ride_day,
  rbd.rider_cancel_after_accept_reserve_cnt_ride_day,
  rbd.driver_cancel_reserve_cnt_ride_day,
  rbd.admin_cancel_reserve_cnt_ride_day,
  rbd.revenue,
  rbd.net_revenue,
  rbd.discount,
  dr.work_driver_cnt,
  dr.drop_driver_cnt,
  dr.reserve_work_driver_cnt,
  dr.work_driver_drop_ride_cnt,
  dr.work_driver_drop_reservation_ride_cnt,
  dr.avg_reserve_work_ratio,
  wr.avg_realtime_dispatch_cnt_in_reserve_working_by_dispatch_logic,
  wr.avg_realtime_drop_cnt_in_reserve_working_by_dispatch_logic,
  wr.rev_per_working_hour_by_dispatch_logic,
  wr.avg_realtime_dispatch_cnt_in_reserve_working_by_std,
  wr.avg_realtime_drop_cnt_in_reserve_working_by_std,
  wr.rev_per_working_hour_by_std
FROM dummy AS d
LEFT JOIN reservation_by_reserveday AS rbr ON d.date_kr = rbr.date_kr AND d.type = rbr.ride_type
LEFT JOIN reservation_by_dday AS rbd ON d.date_kr = rbd.date_kr ANd d.type =rbd.ride_type
LEFT JOIN work_revenue AS wr ON d.date_kr = wr.date_kr AND d.type = wr.type
LEFT JOIN work_driver AS dr ON d.date_kr = dr.date_kr AND d.type = dr.type