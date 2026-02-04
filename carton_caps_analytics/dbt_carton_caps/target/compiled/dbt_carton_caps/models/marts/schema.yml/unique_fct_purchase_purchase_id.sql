
    
    

select
    purchase_id as unique_field,
    count(*) as n_records

from "carton_caps_20260203T224751Z"."main"."fct_purchase"
where purchase_id is not null
group by purchase_id
having count(*) > 1


