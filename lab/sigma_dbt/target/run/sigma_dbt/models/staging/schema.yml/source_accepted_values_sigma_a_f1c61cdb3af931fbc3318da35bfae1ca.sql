
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

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



  
  
      
    ) dbt_internal_test