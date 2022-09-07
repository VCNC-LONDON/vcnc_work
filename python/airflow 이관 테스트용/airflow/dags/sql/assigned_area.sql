SELECT
  name, lng, lat, sido_nm, sgg_nm
FROM (
  SELECT
    name, lng, lat,
    row_number() over (partition by name order by start_at desc) as rn
  FROM (
    SELECT
      assigned_area_name as name,
      assigned_area_lng as lng,
      assigned_area_lat as lat,
      start_at,
    FROM
      tada.driver_schedule

    union all

    SELECT
      assigned_area_name as name,
      assigned_area_lng as lng,
      assigned_area_lat as lat,
      start_at,
    FROM
      tada.driver_assigned_area_history
  )
  WHERE
    name is not null
) t
  JOIN tada_meta.south_korea_shp on st_contains(st_geogfromtext(geometry), st_geogpoint(lng, lat))
WHERE
  rn = 1
