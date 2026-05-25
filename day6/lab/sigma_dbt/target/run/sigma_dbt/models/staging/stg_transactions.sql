
  create or replace   view SIGMA_DE.PUBLIC.stg_transactions
  
  
  
  
  as (
    with source_transactions as (

    select
        transaction_id,
        amount,
        status,
        merchant_id,
        customer_id,
        transaction_date,
        payment_method
    from SIGMA_DE.PUBLIC.fact_transactions
    where merchant_id not like 'TEST_%'

),

cleaned_transactions as (

    select
        cast(transaction_id as varchar) as transaction_id,
        cast(amount as decimal(10, 2)) as amount,
        cast(status as varchar) as status,
        cast(merchant_id as varchar) as merchant_id,
        cast(customer_id as varchar) as customer_id,
        cast(transaction_date as date) as transaction_date,
        cast(payment_method as varchar) as payment_method,
        current_timestamp() as loaded_at
    from source_transactions

)

select * from cleaned_transactions
  );

