
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select failed_count
from SIGMA_DE.PUBLIC.mart_merchant_performance
where failed_count is null



  
  
      
    ) dbt_internal_test