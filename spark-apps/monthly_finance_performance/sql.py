

def process_monthly_finance_performance(spark):
    monthly_finance_performance = spark.sql("""
        -- dim_montlhy_finance_performance 
        --  unit_sold 
        with uss as (
        select 
        date(date_trunc('month', fs2.order_date)) as month,
        sum(quantity) unit_sold
        from fact_sales fs2 
        group by 1
        ),
        -- gmv 
        gmv as (
        select 
        date(date_trunc('month', fs2.order_date)) as month,
        sum(quantity * price) amount
        from fact_sales fs2 
        group by 1
        ),
        -- total outcome
        outcome as (
        select 
        date(date_trunc('month', st.date)) as month,
        sum(amount) amount
        from sum_transactions st 
        where type = 'outcome'
        group by 1
        ),
        -- revenue 
        revenue as (
        select
        date(date_trunc('month', st.date)) as month,
        sum(amount) amount
        from sum_transactions st 
        where type = 'income' and sales_id is not null 
        group by 1
        ),
        -- gross profit
        gross_profit as (
        select 
        date(date_trunc('month', fs2.order_date)) as month,
        sum((fs2.price * tp.profit  / 100) * fs2.quantity)  as amount
        from fact_sales fs2 
        left join tbl_product tp 
        on tp.id  = fs2.product_id 
        group by 1
        ),
        -- net_profit
        net_profit as (
        select 
        gp.month,
        (gp.amount - o.amount) as amount
        from gross_profit gp
        left join outcome o
        on  o.month = gp.month
        )
        select 
        u.month,
        u.unit_sold,
        g.amount as gmv,
        o.amount as outcome,
        r.amount as revenue,
        gp.amount as gross_profit,
        np.amount as nett_profit
        from uss u
        left join gmv g
        on g.month = u.month
        left join outcome o
        on o.month = u.month
        left join revenue r
        on r.month = u.month
        left join gross_profit gp 
        on gp.month = u.month 
        left join net_profit np 
        on np.month = u.month""")
    return monthly_finance_performance
