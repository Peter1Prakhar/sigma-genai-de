
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

    select *
    from SIGMA_DE.PUBLIC.mart_merchant_performance
    where avg_transaction_value < 0


  
  
      
    ) dbt_internal_test