
    
    

select
    product_id as unique_field,
    count(*) as n_records

from "carton_caps_20260203T224751Z"."main"."stg_products"
where product_id is not null
group by product_id
having count(*) > 1


