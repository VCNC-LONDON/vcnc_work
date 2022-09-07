    WITH dispatch_data as (
        SELECT
            id as dispatch_id,
            ride_id,
            driver_id,
            status as dispatch_status,
            created_at
        FROM
            tada.ride_dispatch
        WHERE
            date_kr BETWEEN DATE_SUB("{target_date}", INTERVAL 1 DAY) AND DATE_ADD("{target_date}", INTERVAL 1 DAY)
    
    ), ride_data as (
        SELECT
            ride_id,
            status as ride_status,
            date_kr,
            origin_region,
            case type
                when 'PREMIUM' then 'PLUS'
                when 'NXT' then 'NEXT'
                else type
            end as type,
            cancellation_cause
        FROM
            `kr-co-vcnc-tada.tada_store.ride_base`
        WHERE
            date_kr BETWEEN DATE_SUB("{target_date}", INTERVAL 1 DAY) AND DATE_ADD("{target_date}", INTERVAL 1 DAY)
        
    ), join_dispatch_n_ride as (
        SELECT
            r.ride_id,
            dispatch_id,
            date_kr,
            ride_status,
            dispatch_status,
            cancellation_cause,
            case
            when origin_region in ('서울','부산','성남') then origin_region
                  else '그외' end as origin_region,
            type,
            if(driver_id is null,'매치안됨','매치됨') as matched_or_not,
            ROW_NUMBER() OVER (PARTITION BY r.ride_id ORDER BY created_at desc) as rn
        FROM ride_data r
        LEFT JOIN dispatch_data d
        on r.ride_id = d.ride_id
    
    ), final as (
        SELECT
            date_kr,
            origin_region,
            type,
            count(distinct if(ride_status='CANCELED'and cancellation_cause = 'DISPATCH_TIMEOUT'  and rn=1,ride_id,null)) as  timeout_before_dispatch,
            count(distinct if(dispatch_status is null and matched_or_not='매치안됨' and ride_status='CANCELED'and cancellation_cause='RIDER_CANCELLED' and rn=1,ride_id,null)) as  rider_cancelled_before_dispatch,
            count(distinct if(dispatch_status ='RIDE_CANCELED' and matched_or_not='매치됨' and ride_status='CANCELED'and cancellation_cause='RIDER_CANCELLED' and rn=1,ride_id,null)) as  rider_cancelled_while_dispatching,
            count(distinct if(dispatch_status ='ACCEPTED' and matched_or_not='매치됨' and ride_status='CANCELED'and cancellation_cause IN ('RIDER_CANCELLED','ADMIN_CANCELLED') and rn=1,ride_id,null)) as  rider_cancelled_after_accepted,
            count(distinct if(dispatch_status ='ACCEPTED' and matched_or_not='매치됨' and ride_status='DROPPED_OFF'and cancellation_cause is null and rn=1,ride_id,null)) as  dropped_off,
            count(distinct if(dispatch_status in ('DISPATCH_EXPIRED','PENDING_EXPIRED') and matched_or_not='매치됨' and rn=1,ride_id,null)) as driver_not_accept,
            count(distinct if(dispatch_status ='REJECTED' and matched_or_not='매치됨' and rn=1,ride_id,null)) as  driver_cancelled_while_dispatching,
            count(distinct if(dispatch_status ='ACCEPTED' and matched_or_not='매치됨' and ride_status='CANCELED'and cancellation_cause='DRIVER_CANCELLED' and rn=1,ride_id,null)) as  driver_cancelled_after_accepted,
            count(if(dispatch_status in ('DISPATCH_EXPIRED','PENDING_EXPIRED') and matched_or_not='매치됨',dispatch_id,null)) as  driver_not_accept_dispatch,
            count(if(dispatch_status ='REJECTED' and matched_or_not='매치됨',dispatch_id,null)) as driver_cancelled_while_dispatching_dispatch,
        FROM
            join_dispatch_n_ride
        WHERE
            date_kr BETWEEN DATE_SUB("{target_date}", INTERVAL 1 DAY) AND DATE_ADD("{target_date}", INTERVAL 1 DAY)
        GROUP BY
            date_kr,
            origin_region,
            type
    )
    
    SELECT *
    FROM final
    WHERE date_kr = "{target_date}"