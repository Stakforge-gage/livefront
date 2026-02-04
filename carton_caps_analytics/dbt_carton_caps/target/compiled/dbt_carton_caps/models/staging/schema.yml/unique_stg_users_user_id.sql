
    
    

select
    user_id as unique_field,
    count(*) as n_records

from "carton_caps_20260203T224751Z"."main"."stg_users"
where user_id is not null
group by user_id
having count(*) > 1


