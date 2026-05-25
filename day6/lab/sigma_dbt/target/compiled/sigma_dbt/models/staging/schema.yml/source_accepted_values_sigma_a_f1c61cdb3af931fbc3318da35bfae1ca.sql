
    
    

with all_values as (

    select
        tier as value_field,
        count(*) as n_records

    from SIGMA_DE.PUBLIC.dim_customer
    group by tier

)

select *
from all_values
where value_field not in (
    'GOLD','SILVER','BRONZE'
)


