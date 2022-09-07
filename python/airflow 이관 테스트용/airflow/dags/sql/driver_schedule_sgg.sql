select
  vehicle_id,
  driver.type as driver_type,
  start_at,
  end_at,
  concat(substr(shp.sido_nm, 0, 2), ' ', shp.sgg_nm) as vehicle_zone,
  concat(substr(assigned_area.sido_nm, 0, 2), ' ', assigned_area.sgg_nm) as assigned_area
from
  tada.driver_schedule
  join tada.driver on driver_id = driver.id
  join tada.vehicle_zone on vehicle_zone_id = vehicle_zone.id
  join tada_meta.south_korea_shp shp on st_contains(st_geogfromtext(shp.geometry), st_geogpoint(location_lng, location_lat))
  join tada_meta.assigned_area on driver_schedule.assigned_area_name = assigned_area.name
