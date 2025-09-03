
def process_finance_performance(spark):
    finance_performance = spark.sql("""
        with uss as (
        select 
        sum(quantity) unit_sold
        from fact_sales fs2 
        ),
        -- gmv 
        gmv as (
        select 
        sum(quantity * price) amount
        from fact_sales fs2 
        ),
        -- total outcome
        outcome as (
        select 
        sum(amount) amount
        from sum_transactions st 
        where type = 'outcome'
        ),
        -- revenue 
        revenue as (
        select
        sum(amount) amount
        from sum_transactions st 
        where type = 'income' and sales_id is not null 
        ),
        -- gross profit
        gross_profit as (
        select 
        sum((fs2.price * tp.profit  / 100) * fs2.quantity)  as amount
        from fact_sales fs2 
        left join tbl_product tp 
        on tp.id  = fs2.product_id 
        ),
        -- net_profit
        net_profit as (
        select (select amount from gross_profit) - (select amount from outcome) as amount
        )
        SELECT 
            (SELECT unit_sold FROM uss) AS unit_sold,
            (SELECT amount FROM outcome) AS outcome,
            (SELECT amount FROM gmv) AS gmv, 
            (SELECT amount FROM revenue) AS revenue,
            (SELECT amount FROM gross_profit) AS gross_profit,
            (SELECT amount FROM net_profit) AS net_profit """)
    return finance_performance