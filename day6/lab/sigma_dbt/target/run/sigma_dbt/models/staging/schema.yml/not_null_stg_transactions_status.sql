
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select status
from SIGMA_DE.PUBLIC.stg_transactions
where status is null



  
  
      
    ) dbt_internal_test