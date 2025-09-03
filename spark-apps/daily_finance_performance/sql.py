

def process_daily_finance_performance(spark):
    daily_finance_performance = spark.sql("""
        --dims_daily_finance_performance
        --  unit_sold 
        with uss as (
        select 
        date(fs2.order_date) as date,
        sum(quantity) unit_sold
        from fact_sales fs2 
        group by 1
        ),
        -- gmv 
        gmv as (
        select 
        date(fs2.order_date) as date,
        sum(quantity * price) amount
        from fact_sales fs2 
        group by 1
        ),
        -- total outcome
        outcome as (
        select 
        date(st.date) as date,
        sum(amount) amount
        from sum_transactions st 
        where type = 'outcome'
        group by 1
        ),
        -- revenue 
        revenue as (
        select
        date(st.date) as date,
        sum(amount) amount
        from sum_transactions st 
        where type = 'income' and sales_id is not null 
        group by 1
        ),
        -- gross profit
        gross_profit as (
        select 
        date(fs2.order_date) as date,
        sum((fs2.price * tp.profit  / 100) * fs2.quantity)  as amount
        from fact_sales fs2 
        left join tbl_product tp 
        on tp.id  = fs2.product_id 
        group by 1
        ),
        -- net_profit
        net_profit as (
        select 
        gp.date,
        (gp.amount - o.amount) as amount
        from gross_profit gp
        left join outcome o
        on  o.date = gp.date
        )
        select 
        u.date,
        u.unit_sold,
        g.amount as gmv,
        o.amount as outcome,
        r.amount as revenue,
        gp.amount as gross_profit,
        np.amount as nett_profit
        from uss u
        left join gmv g
        on g.date = u.date
        left join outcome o
        on o.date = u.date
        left join revenue r
        on r.date = u.date
        left join gross_profit gp 
        on gp.date = u.date 
        left join net_profit np 
        on np.date = u.date""")
    return daily_finance_performance
