select
  date_kr, timestamp_trunc(timestamp_millis(time_ms), hour) as date_hour,
  sum(if(trip.type is null and driver.type = 'BASIC', distance_delta_meters, 0)) as meters_basic,
  sum(if(trip.type is null and driver.type = 'BASIC' and a.activity_status = 'IDLE', distance_delta_meters, 0)) as meters_basic_idle,
  sum(if(trip.type is null and driver.type = 'BASIC' and a.activity_status = 'DISPATCHING', distance_delta_meters, 0)) as meters_basic_dispatching,
  sum(if(trip.type is null and driver.type = 'BASIC' and a.activity_status = 'RIDING', distance_delta_meters, 0)) as meters_basic_riding,
  sum(if(trip.type is null and driver.type = 'BASIC' and a.activity_status = 'RIDING' and a.ride_status = 'ACCEPTED', distance_delta_meters, 0)) as meters_basic_riding_accepted,
  sum(if(trip.type is null and driver.type = 'BASIC' and a.activity_status = 'RIDING' and a.ride_status = 'ARRIVED', distance_delta_meters, 0)) as meters_basic_riding_arrived,
  sum(if(trip.type is null and driver.type = 'BASIC' and a.activity_status = 'RIDING' and a.ride_status = 'PICKED_UP', distance_delta_meters, 0)) as meters_basic_riding_picked_up,
  sum(if(driver.type = 'ASSIST', distance_delta_meters, 0)) as meters_assist,
  sum(if(driver.type = 'ASSIST' and a.activity_status = 'IDLE', distance_delta_meters, 0)) as meters_assist_idle,
  sum(if(driver.type = 'ASSIST' and a.activity_status = 'DISPATCHING', distance_delta_meters, 0)) as meters_assist_dispatching,
  sum(if(driver.type = 'ASSIST' and a.activity_status = 'RIDING', distance_delta_meters, 0)) as meters_assist_riding,
  sum(if(driver.type = 'ASSIST' and a.activity_status = 'RIDING' and a.ride_status = 'ACCEPTED', distance_delta_meters, 0)) as meters_assist_riding_accepted,
  sum(if(driver.type = 'ASSIST' and a.activity_status = 'RIDING' and a.ride_status = 'ARRIVED', distance_delta_meters, 0)) as meters_assist_riding_arrived,
  sum(if(driver.type = 'ASSIST' and a.activity_status = 'RIDING' and a.ride_status = 'PICKED_UP', distance_delta_meters, 0)) as meters_assist_riding_picked_up,
  sum(if(driver.type = 'PREMIUM', distance_delta_meters, 0)) as meters_premium,
  sum(if(driver.type = 'PREMIUM' and a.activity_status = 'IDLE', distance_delta_meters, 0)) as meters_premium_idle,
  sum(if(driver.type = 'PREMIUM' and a.activity_status = 'DISPATCHING', distance_delta_meters, 0)) as meters_premium_dispatching,
  sum(if(driver.type = 'PREMIUM' and a.activity_status = 'RIDING', distance_delta_meters, 0)) as meters_premium_riding,
  sum(if(driver.type = 'PREMIUM' and a.activity_status = 'RIDING' and a.ride_status = 'ACCEPTED', distance_delta_meters, 0)) as meters_premium_riding_accepted,
  sum(if(driver.type = 'PREMIUM' and a.activity_status = 'RIDING' and a.ride_status = 'ARRIVED', distance_delta_meters, 0)) as meters_premium_riding_arrived,
  sum(if(driver.type = 'PREMIUM' and a.activity_status = 'RIDING' and a.ride_status = 'PICKED_UP', distance_delta_meters, 0)) as meters_premium_riding_picked_up,
  sum(if(trip.type = 'AIR', distance_delta_meters, 0)) as meters_air,
  sum(if(trip.type = 'AIR' and a.activity_status = 'TRIP_RIDING', distance_delta_meters, 0)) as meters_air_trip_riding,
  sum(if(trip.type = 'AIR' and a.activity_status = 'TRIP_RIDING' and a.trip_status = 'ACCEPTED', distance_delta_meters, 0)) as meters_air_trip_riding_accepted,
  sum(if(trip.type = 'AIR' and a.activity_status = 'TRIP_RIDING' and a.trip_status = 'ARRIVED', distance_delta_meters, 0)) as meters_air_trip_riding_arrived,
  sum(if(trip.type = 'AIR' and a.activity_status = 'TRIP_RIDING' and a.trip_status = 'PICKED_UP', distance_delta_meters, 0)) as meters_air_trip_riding_picked_up,
  sum(if(trip.type = 'CHARTER', distance_delta_meters, 0)) as meters_charter,
  sum(if(trip.type = 'CHARTER' and a.activity_status = 'TRIP_RIDING', distance_delta_meters, 0)) as meters_charter_trip_riding,
  sum(if(trip.type = 'CHARTER' and a.activity_status = 'TRIP_RIDING' and a.trip_status = 'ACCEPTED', distance_delta_meters, 0)) as meters_charter_trip_riding_accepted,
  sum(if(trip.type = 'CHARTER' and a.activity_status = 'TRIP_RIDING' and a.trip_status = 'ARRIVED', distance_delta_meters, 0)) as meters_charter_trip_riding_arrived,
  sum(if(trip.type = 'CHARTER' and a.activity_status = 'TRIP_RIDING' and a.trip_status = 'PICKED_UP', distance_delta_meters, 0)) as meters_charter_trip_riding_picked_up,
  sum(if(trip.type = 'VAN', distance_delta_meters, 0)) as meters_vipvan,
  sum(if(trip.type = 'VAN' and a.activity_status = 'TRIP_RIDING', distance_delta_meters, 0)) as meters_vipvan_trip_riding,
  sum(if(trip.type = 'VAN' and a.activity_status = 'TRIP_RIDING' and a.trip_status = 'ACCEPTED', distance_delta_meters, 0)) as meters_vipvan_trip_riding_accepted,
  sum(if(trip.type = 'VAN' and a.activity_status = 'TRIP_RIDING' and a.trip_status = 'ARRIVED', distance_delta_meters, 0)) as meters_vipvan_trip_riding_arrived,
  sum(if(trip.type = 'VAN' and a.activity_status = 'TRIP_RIDING' and a.trip_status = 'PICKED_UP', distance_delta_meters, 0)) as meters_vipvan_trip_riding_picked_up,
  sum(if(a.activity_status = 'TRIP_RETURNING', distance_delta_meters, 0)) as meters_trip_returning
from
  tada.tracker_vehicle_history tvh
  left join tada.driver on tvh.driver_id = driver.id
  left join (
    select
      driver_id, start_at, end_at, activity_status, ride_status, trip_id, trip_status
    from
      tada.driver_activity
    where
      date(start_at, 'Asia/Seoul') <= '{execute_date}'
      and date(end_at, 'Asia/Seoul') >= '{execute_date}'
  ) a on tvh.driver_id = a.driver_id and timestamp_millis(time_ms) >= start_at and timestamp_millis(time_ms) < end_at
  left join tada.trip on a.trip_id = trip.id
where
  speed_meter_per_second < (200 * 1000 / 3600) -- 200km/h 미만만 선택.
  and date_kr = '{execute_date}'
group by
  date_kr, date_hour