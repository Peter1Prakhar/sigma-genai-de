
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

    select *
    from SIGMA_DE.PUBLIC.mart_merchant_performance
    where failure_rate_pct < 0
       or failure_rate_pct > 100


  
  
      
    ) dbt_internal_test