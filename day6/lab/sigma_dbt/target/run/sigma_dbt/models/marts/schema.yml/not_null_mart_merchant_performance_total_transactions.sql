
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_transactions
from SIGMA_DE.PUBLIC.mart_merchant_performance
where total_transactions is null



  
  
      
    ) dbt_internal_test