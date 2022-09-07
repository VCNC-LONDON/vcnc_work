select
  trip.* except (driver_id, vehicle_id, reservation_type, ride_status),
  ifnull(trip.driver_id, driver_ext.id) as driver_id,
  ifnull(trip.vehicle_id, vehicle.id) as vehicle_id,
  ifnull(reservation_type, 'COMMON') as reservation_type,
  case
    when ride_status is not null then ride_status
    when ifnull(trip.driver_id, driver_ext.id) is not null and cancelled_at is null and pickup_planned_at < current_timestamp() then 'DROPPED_OFF'
  end as ride_status,
  DATE(trip.created_at, "Asia/Seoul") AS created_date_kr
from
  tada.trip
  left join tada_ext.driver_ext on driver_name = driver_ext.name and driver_agency_name = agency_name
  left join tada.vehicle on coalesce(vehicle_license_plate, driver_vehicle_license_plate) = vehicle.license_plate
