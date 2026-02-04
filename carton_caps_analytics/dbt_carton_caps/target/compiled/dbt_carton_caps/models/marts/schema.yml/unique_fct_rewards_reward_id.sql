
    
    

select
    reward_id as unique_field,
    count(*) as n_records

from "carton_caps_20260203T224751Z"."main"."fct_rewards"
where reward_id is not null
group by reward_id
having count(*) > 1


