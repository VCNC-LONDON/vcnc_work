WITH

client_firebase AS (
  SELECT
    date_kr,
    user_id,
    event_name,
    event_datetime,
    MAX(IF(e.key = "view_name", e.string_value, NULL)) AS view_name,
    MAX(IF(e.key = "ga_session_id", e.int_value, NULL)) AS ga_session_id
  FROM (SELECT * EXCEPT(user_id), MAX(user_id) OVER (PARTITION BY user_pseudo_id) AS user_id FROM `kr-co-vcnc-tada.tada_ext.firebase_app_event` WHERE date_kr BETWEEN "2022-07-20" AND "2022-07-26"), UNNEST(event_params) AS e
  WHERE app_id = "kr.co.vcnc.tada.driver"
  AND event_name IN ("view_click", "view_impression")
  AND CAST(REPLACE(app_version, ".","") AS int64) < 280
  GROUP BY 1, 2, 3, 4
  HAVING view_name LIKE "%RESERVATION%"
),

client_raw AS (
  SELECT
    date_kr,
    user_id,  
    event_name,
    (CASE
      WHEN view_name IN ("RESERVATION_LIST_REFRESH") THEN "RESERVATION_LIST_REFRESH"
      WHEN view_name IN ("RIDE_RESERVATION_DETAIL_CANCEL") THEN "RIDE_RESERVATION_DETAIL_CANCEL"
      WHEN view_name IN ("RESERVATION_LIST_RETRY") THEN "RESERVATION_LIST_RETRY"
    END)
  FROM client_firebase
),

web_firebase AS (
  SELECT
    date_kr,
    user_id,
    event_name,
    event_datetime,
    MAX(IF(e.key = "page_location", e.string_value, NULL)) AS page_location,
    MAX(IF(e.key = "component_name", e.string_value, NULL)) AS component_name,
    MAX(IF(e.key = "query_string", e.string_value, NULL)) AS query_string,
    MAX(IF(e.key = "reservation_id", e.string_value, NULL)) AS reservation_id,
    MAX(IF(e.key = "error_code", e.string_value, NULL)) AS error_code,
    MAX(IF(e.key = "ga_session_id", e.int_value, NULL)) AS ga_session_id
  FROM (SELECT * EXCEPT(user_id), MAX(user_id) OVER (PARTITION BY user_pseudo_id) AS user_id FROM `kr-co-vcnc-tada.tada_ext.firebase_app_event` WHERE date_kr >= "2022-07-29"), UNNEST(event_params) AS e
  WHERE app_id = "kr.co.vcnc.tada.driver"
  AND event_name IN ("web_page_view", "web_click", "web_view_impression")
  AND CAST(REPLACE(app_version, ".","") AS int64) >= 280
  GROUP BY 1, 2, 3, 4
),

web_raw AS (
  SELECT    
    date_kr,
    user_id,  
    event_name,
    (CASE
      WHEN page_location = "https://web-driver-web.tadatada.com/reservations" AND event_name = "web_click" AND component_name = "RIDE_HISTORY" THEN "ride_history_income"
      WHEN page_location = "https://web-driver-web.tadatada.com/reservations" AND event_name = "web_page_view" THEN "call_list_income"
      WHEN page_location = "https://web-driver-web.tadatada.com/reservations" AND event_name = "web_click" AND component_name = "FILTER" THEN "filter_click"
      WHEN page_location = "https://web-driver-web.tadatada.com/reservations" AND event_name = "web_click" AND component_name = "RECOMMEND_ASC" THEN "filter_recommend_click"
      WHEN page_location = "https://web-driver-web.tadatada.com/reservations" AND event_name = "web_click" AND component_name = "TIME_ASC" THEN "filter_time_click"
      WHEN page_location = "https://web-driver-web.tadatada.com/reservations" AND event_name = "web_click" AND component_name = "SURGE_ASC" THEN "filter_surge_click"
      WHEN page_location = "https://web-driver-web.tadatada.com/reservations" AND event_name = "web_click" AND component_name = "REFRESH" THEN "call_list_refresh_click"
      WHEN page_location = "https://web-driver-web.tadatada.com/reservations" AND event_name = "web_click" AND component_name = "RESERVATION_CARD" THEN "reserve_select_click"
      WHEN page_location = "https://web-driver-web.tadatada.com/reservations" AND event_name = "web_click" AND component_name = "APP_BAR_HELP_BUTTON" THEN "call_list_help_click"
      WHEN page_location = "https://web-driver-web.tadatada.com/reservations" AND event_name = "web_view_impression" THEN "reserve_select_error"
      WHEN page_location LIKE "https://web-driver-web.tadatada.com/reservations/%" AND event_name = "web_page_view" THEN "reserve_info_income"
      WHEN page_location LIKE "https://web-driver-web.tadatada.com/reservations/%" AND event_name = "web_click" AND query_string LIKE "reservationType%" AND component_name = "ACCEPT_RIDE_RESERVATION" THEN "reserve_info_reserve_click"
      -- WHEN page_location LIKE "https://web-driver-web.tadatada.com/reservations/%" AND event_name = "web_click" AND query_string LIKE "reservationType%" AND component_name IN ("CONFIRM","CANCEL") THEN "reserve_info_reserve_confirm_click"
      WHEN page_location LIKE "https://web-driver-web.tadatada.com/reservations/%" AND event_name = "web_click" AND query_string LIKE "reservationType%" AND component_name = "CONFIRM" THEN "reserve_info_reserve_ok_click"
      WHEN page_location LIKE "https://web-driver-web.tadatada.com/reservations/%" AND event_name = "web_click" AND query_string LIKE "reservationType%" AND component_name = "CANCEL" THEN "reserve_info_reserve_no_click"
      WHEN page_location LIKE "https://web-driver-web.tadatada.com/reservations/%" AND event_name = "web_view_impression" AND query_string LIKE "reservationType%" THEN "reserve_info_error"
    END) AS call_list_event_name,
    event_datetime,
    query_string,
    page_location,
    component_name,
    reservation_id,
    error_code,
    IF(DATETIME_DIFF(event_datetime, LAG(event_datetime) OVER (PARTITION BY user_id ORDER BY event_datetime) , SECOND) / 60 > 15 OR LAG(event_datetime) OVER (PARTITION BY user_id ORDER BY event_datetime) IS NULL , True, False) AS is_new_session,
    ga_session_id
  FROM web_firebase 
  WHERE page_location LIKE "https://web-driver-web.tadatada.com/reservations%"
),

sessionize_web_raw AS (
  SELECT
    * EXCEPT(session_rn, query_string),
    SUBSTR(SPLIT(query_string, "&")[SAFE_ORDINAL(1)] , STRPOS(SPLIT(query_string, "&")[SAFE_ORDINAL(1)],"=")+1, LENGTH(SPLIT(query_string, "&")[SAFE_ORDINAL(1)])) AS tab,
    SUBSTR(SPLIT(query_string, "&")[SAFE_ORDINAL(2)] , STRPOS(SPLIT(query_string, "&")[SAFE_ORDINAL(2)],"=")+1, LENGTH(SPLIT(query_string, "&")[SAFE_ORDINAL(2)])) AS sort,
    SUBSTR(SPLIT(query_string, "&")[SAFE_ORDINAL(3)] , STRPOS(SPLIT(query_string, "&")[SAFE_ORDINAL(3)],"=")+1, LENGTH(SPLIT(query_string, "&")[SAFE_ORDINAL(3)])) AS order_rule,
    MAX(session_rn) OVER (PARTITION BY user_id ORDER BY event_datetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
  FROM (
    SELECT * EXCEPT(is_new_session), IF(is_new_session, ROW_NUMBER() OVER (PARTITION BY user_id, is_new_session ORDER BY event_datetime), NULL) AS session_rn FROM web_raw
  )
)

-- SELECT DISTINCT view_name FROM client_firebase
SELECT * FROM sessionize_web_raw where call_list_event_name is null ORDER BY user_id, event_datetime
