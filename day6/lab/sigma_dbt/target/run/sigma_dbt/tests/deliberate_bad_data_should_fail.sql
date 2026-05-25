
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Training exercise: this singular test intentionally fails so the
-- validator can confirm dbt test failures are visible in run_results.json.
select
    'CANCELLED status should be rejected by the data contract' as failure_reason
  
  
      
    ) dbt_internal_test