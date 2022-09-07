select
  driver_id, start_at,
  min(working_start_at) as working_start_at,
  max(working_end_at) as working_end_at,
from (
  select
    s.driver_id, s.start_at, working_start_at, working_end_at,
    row_number() over (partition by w.driver_id, working_start_at order by s.start_at desc) as rn, -- DriverScheduleService#getStartWorkingSchedule 의 로직대로.
  from (
    select
      driver_id, start_at as working_start_at, working_end_at
    from (
      select
        driver_id, activity_status, start_at, activity_status_prev,
        min(if(activity_status_next = 'OFF', end_at, null)) over (partition by driver_id order by seq_id rows between current row and unbounded following) as working_end_at
      from (
        select
          driver_id, seq_id, activity_status, start_at, end_at,
          lag(activity_status, 1) over (partition by driver_id order by seq_id) as activity_status_prev,
          lead(activity_status, 1) over (partition by driver_id order by seq_id) as activity_status_next
        from
          tada.driver_activity
      )
      where
        activity_status_prev = 'OFF' or activity_status_next = 'OFF'
    )
    where
      activity_status_prev = 'OFF'
  ) w
    join tada.driver_schedule s on w.driver_id = s.driver_id
      and working_start_at > timestamp_sub(s.start_at, interval 3 hour) -- DriverScheduleService#getStartWorkingSchedule 의 로직대로.
      and working_start_at < timestamp_add(s.end_at, interval 3 hour)   -- DriverScheduleService#getStartWorkingSchedule 의 로직대로.
)
where
  rn = 1
group by
  driver_id, start_at
