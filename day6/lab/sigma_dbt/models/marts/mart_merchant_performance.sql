with transactions as (

    select
        transaction_id,
        amount,
        status,
        merchant_id,
        customer_id
    from {{ ref('stg_transactions') }}

),

merchant as (

    select
        merchant_id,
        merchant_name,
        category,
        city,
        onboarded_date
    from {{ source('sigma_analytics', 'dim_merchant') }}

),

merchant_metrics as (

    select
        merchant_id,
        count(*) as total_transactions,
        sum(case when status = 'FAILED' then 1 else 0 end) as failed_count,
        sum(case when status = 'COMPLETED' then amount else 0 end) as total_revenue,
        avg(case when status = 'COMPLETED' then amount end) as avg_transaction_value,
        count(distinct customer_id) as unique_customers
    from transactions
    group by merchant_id

),

final as (

    select
        merchant.merchant_id,
        merchant.merchant_name,
        merchant.category,
        merchant.city,
        merchant.onboarded_date,
        coalesce(merchant_metrics.total_revenue, 0) as total_revenue,
        coalesce(merchant_metrics.total_transactions, 0) as total_transactions,
        coalesce(merchant_metrics.failed_count, 0) as failed_count,
        round(
            100.0 * coalesce(merchant_metrics.failed_count, 0)
            / nullif(merchant_metrics.total_transactions, 0),
            2
        ) as failure_rate_pct,
        coalesce(merchant_metrics.avg_transaction_value, 0) as avg_transaction_value,
        coalesce(merchant_metrics.unique_customers, 0) as unique_customers
    from merchant
    left join merchant_metrics
        on merchant.merchant_id = merchant_metrics.merchant_id

)

select * from final
