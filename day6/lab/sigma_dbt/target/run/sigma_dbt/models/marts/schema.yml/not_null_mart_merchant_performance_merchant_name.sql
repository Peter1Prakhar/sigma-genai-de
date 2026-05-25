
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select merchant_name
from SIGMA_DE.PUBLIC.mart_merchant_performance
where merchant_name is null



  
  
      
    ) dbt_internal_test