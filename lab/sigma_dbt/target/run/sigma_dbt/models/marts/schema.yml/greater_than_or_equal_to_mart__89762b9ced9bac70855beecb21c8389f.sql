
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

    select *
    from SIGMA_DE.PUBLIC.mart_merchant_performance
    where failed_count < 0


  
  
      
    ) dbt_internal_test