WITH

event_params AS (
  SELECT 
    * EXCEPT(event_params),
    ARRAY_AGG(STRUCT(event_params.key, event_params.value.string_value, event_params.value.int_value, event_params.value.float_value, event_params.value.double_value)) AS  event_params
  FROM (
    SELECT 
      DATE(TIMESTAMP_MICROS(event_timestamp), "Asia/Seoul") AS date_kr,
      user_id,
      user_pseudo_id,
      event_name,
      DATETIME(TIMESTAMP_MICROS(event_timestamp), "Asia/Seoul") AS event_datetime,
      DATETIME(TIMESTAMP_MICROS(user_first_touch_timestamp), "Asia/Seoul") AS first_touch_datetime,
      platform,
      event_params,
      device.mobile_brand_name as mobile_brand_name,
      device.mobile_model_name as mobile_model_name, 
      device.advertising_id as advertising_id,
      device.language as device_language,
      geo.continent as continent,
      geo.country as country,
      geo.region as region,
      geo.city as city,
      app_info.version as app_version,
      app_info.id as app_id
    FROM `kr-co-vcnc-tada.analytics_181161192.events_*`, UNNEST(event_params) as event_params
    WHERE event_params.key NOT IN ("firebase_event_origin", "firebase_screen_id", "engaged_session_event", "firebase_previous_class", "firebase_previous_id")
    AND event_name NOT IN ("network_status", "tmap_route")
    AND _TABLE_SUFFIX = "{execute_date}"
  )
  GROUP BY
    date_kr, event_datetime, event_name, user_id, user_pseudo_id, mobile_brand_name, mobile_model_name,
    app_version, first_touch_datetime, platform, mobile_brand_name, mobile_model_name, 
    advertising_id, device_language, continent, country, region, city, app_version, app_id 
),

user_properties AS (
  SELECT 
    * EXCEPT(key, string_value, int_value, float_value, double_value ),
    ARRAY_AGG(STRUCT(key, string_value, int_value, float_value, double_value)) AS  user_property
  FROM (
    SELECT 
      DISTINCT DATE(TIMESTAMP_MICROS(event_timestamp), "Asia/Seoul") AS date_kr,
      user_id,
      user_pseudo_id,
      event_name,
      DATETIME(TIMESTAMP_MICROS(event_timestamp), "Asia/Seoul") AS event_datetime,
      user_properties.key AS key,
      user_properties.value.string_value AS string_value,
      user_properties.value.int_value AS int_value,
      user_properties.value.float_value AS float_value,
      user_properties.value.double_value AS double_value,
    FROM `kr-co-vcnc-tada.analytics_181161192.events_*`, UNNEST(user_properties) AS user_properties
    WHERE  user_properties.key NOT IN ("ga_session_id", "ga_session_number", "first_open_time")
    AND user_properties.key NOT LIKE "firebase_exp_%"
    AND event_name NOT IN ("network_status", "tmap_route")
    AND _TABLE_SUFFIX = "{execute_date}"
  )
  GROUP BY
    date_kr, event_datetime, event_name, user_id, user_pseudo_id 
)

SELECT 
  e.*,
  u.user_property 
FROM event_params AS e
LEFT JOIN user_properties AS u
USING(date_kr, event_datetime, event_name, user_id, user_pseudo_id )