select
  date_hour,

  sum(if(trip_type is null and driver_type = 'BASIC', seconds, 0)) as seconds_basic,
  sum(if(trip_type is null and driver_type = 'BASIC' and activity_status = 'IDLE', seconds, 0)) as seconds_basic_idle,
  sum(if(trip_type is null and driver_type = 'BASIC' and activity_status = 'DISPATCHING', seconds, 0)) as seconds_basic_dispatching,
  sum(if(trip_type is null and driver_type = 'BASIC' and activity_status = 'RIDING', seconds, 0)) as seconds_basic_riding,
  sum(if(trip_type is null and driver_type = 'BASIC' and activity_status = 'RIDING' and ride_status = 'ACCEPTED', seconds, 0)) as seconds_basic_riding_accepted,
  sum(if(trip_type is null and driver_type = 'BASIC' and activity_status = 'RIDING' and ride_status = 'ARRIVED', seconds, 0)) as seconds_basic_riding_arrived,
  sum(if(trip_type is null and driver_type = 'BASIC' and activity_status = 'RIDING' and ride_status = 'PICKED_UP', seconds, 0)) as seconds_basic_riding_picked_up,

  sum(if(driver_type = 'ASSIST', seconds, 0)) as seconds_assist,
  sum(if(driver_type = 'ASSIST' and activity_status = 'IDLE', seconds, 0)) as seconds_assist_idle,
  sum(if(driver_type = 'ASSIST' and activity_status = 'DISPATCHING', seconds, 0)) as seconds_assist_dispatching,
  sum(if(driver_type = 'ASSIST' and activity_status = 'RIDING', seconds, 0)) as seconds_assist_riding,
  sum(if(driver_type = 'ASSIST' and activity_status = 'RIDING' and ride_status = 'ACCEPTED', seconds, 0)) as seconds_assist_riding_accepted,
  sum(if(driver_type = 'ASSIST' and activity_status = 'RIDING' and ride_status = 'ARRIVED', seconds, 0)) as seconds_assist_riding_arrived,
  sum(if(driver_type = 'ASSIST' and activity_status = 'RIDING' and ride_status = 'PICKED_UP', seconds, 0)) as seconds_assist_riding_picked_up,

  sum(if(driver_type = 'PREMIUM', seconds, 0)) as seconds_premium,
  sum(if(driver_type = 'PREMIUM' and activity_status = 'IDLE', seconds, 0)) as seconds_premium_idle,
  sum(if(driver_type = 'PREMIUM' and activity_status = 'DISPATCHING', seconds, 0)) as seconds_premium_dispatching,
  sum(if(driver_type = 'PREMIUM' and activity_status = 'RIDING', seconds, 0)) as seconds_premium_riding,
  sum(if(driver_type = 'PREMIUM' and activity_status = 'RIDING' and ride_status = 'ACCEPTED', seconds, 0)) as seconds_premium_riding_accepted,
  sum(if(driver_type = 'PREMIUM' and activity_status = 'RIDING' and ride_status = 'ARRIVED', seconds, 0)) as seconds_premium_riding_arrived,
  sum(if(driver_type = 'PREMIUM' and activity_status = 'RIDING' and ride_status = 'PICKED_UP', seconds, 0)) as seconds_premium_riding_picked_up,

  sum(if(trip_type = 'AIR', seconds, 0)) as seconds_air,
  sum(if(trip_type = 'AIR' and activity_status = 'TRIP_RIDING', seconds, 0)) as seconds_air_trip_riding,
  sum(if(trip_type = 'AIR' and activity_status = 'TRIP_RIDING' and trip_status = 'ACCEPTED', seconds, 0)) as seconds_air_trip_riding_accepted,
  sum(if(trip_type = 'AIR' and activity_status = 'TRIP_RIDING' and trip_status = 'ARRIVED', seconds, 0)) as seconds_air_trip_riding_arrived,
  sum(if(trip_type = 'AIR' and activity_status = 'TRIP_RIDING' and trip_status = 'PICKED_UP', seconds, 0)) as seconds_air_trip_riding_picked_up,

  sum(if(trip_type = 'CHARTER', seconds, 0)) as seconds_charter,
  sum(if(trip_type = 'CHARTER' and activity_status = 'TRIP_RIDING', seconds, 0)) as seconds_charter_trip_riding,
  sum(if(trip_type = 'CHARTER' and activity_status = 'TRIP_RIDING' and trip_status = 'ACCEPTED', seconds, 0)) as seconds_charter_trip_riding_accepted,
  sum(if(trip_type = 'CHARTER' and activity_status = 'TRIP_RIDING' and trip_status = 'ARRIVED', seconds, 0)) as seconds_charter_trip_riding_arrived,
  sum(if(trip_type = 'CHARTER' and activity_status = 'TRIP_RIDING' and trip_status = 'PICKED_UP', seconds, 0)) as seconds_charter_trip_riding_picked_up,

  sum(if(trip_type = 'VAN', seconds, 0)) as seconds_vipvan,
  sum(if(trip_type = 'VAN' and activity_status = 'TRIP_RIDING', seconds, 0)) as seconds_vipvan_trip_riding,
  sum(if(trip_type = 'VAN' and activity_status = 'TRIP_RIDING' and trip_status = 'ACCEPTED', seconds, 0)) as seconds_vipvan_trip_riding_accepted,
  sum(if(trip_type = 'VAN' and activity_status = 'TRIP_RIDING' and trip_status = 'ARRIVED', seconds, 0)) as seconds_vipvan_trip_riding_arrived,
  sum(if(trip_type = 'VAN' and activity_status = 'TRIP_RIDING' and trip_status = 'PICKED_UP', seconds, 0)) as seconds_vipvan_trip_riding_picked_up,

  sum(if(activity_status = 'TRIP_RETURNING', seconds, 0)) as seconds_trip_returning
from (
  select
    date_hour, driver_type, trip_type, activity_status, ride_status, trip_status,
    timestamp_diff(
      least(timestamp_add(date_hour, interval 1 hour), end_at),
      greatest(date_hour, start_at),
      second
    ) as seconds
  from (
    select
      a.activity_status, a.ride_status, a.trip_status, a.start_at, ifnull(a.end_at, current_timestamp()) as end_at,
      driver.type as driver_type,
      trip.type as trip_type
    from
      tada.driver_activity a
      left join tada.driver on a.driver_id = driver.id
      left join tada.trip on a.trip_id = trip.id
    where
      a.activity_status in ('IDLE', 'DISPATCHING', 'RIDING', 'TRIP_RIDING', 'TRIP_RETURNING')
  ), unnest(
    generate_timestamp_array(
      timestamp_trunc(start_at, hour),
      timestamp_trunc(end_at, hour),
      interval 1 hour
    )
  ) as date_hour
)
where
  date(date_hour, 'Asia/Seoul') >= '2019-07-15'
group by
  date_hour
