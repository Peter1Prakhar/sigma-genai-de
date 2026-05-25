

    select *
    from SIGMA_DE.PUBLIC.mart_merchant_performance
    where failure_rate_pct < 0
       or failure_rate_pct > 100

