

def process_weekly_finance_performance(spark):
    weekly_finance_performance = spark.sql(""" 
    -- dims_weakly_finance_performance 
    --  unit_sold 
    with uss as (
    select 
    date(DATE '2024-12-29' + INTERVAL '1 day' * FLOOR(EXTRACT(DAY FROM fs2.order_date - DATE '2024-12-29') / 7) * 7) AS week,
    sum(quantity) unit_sold
    from fact_sales fs2 
    group by 1
    ),
    -- gmv 
    gmv as (
    select 
    date(DATE '2024-12-29' + INTERVAL '1 day' * FLOOR(EXTRACT(DAY FROM fs2.order_date - DATE '2024-12-29') / 7) * 7) AS week,
    sum(quantity * price) amount
    from fact_sales fs2 
    group by 1
    ),
    -- total outcome
    outcome as (
    select 
    date(DATE '2024-12-29' + INTERVAL '1 day' * FLOOR(EXTRACT(DAY FROM st.date - DATE '2024-12-29') / 7) * 7) AS week,
    sum(amount) amount
    from sum_transactions st 
    where type = 'outcome'
    group by 1
    ),
    -- revenue 
    revenue as (
    select
    date(DATE '2024-12-29' + INTERVAL '1 day' * FLOOR(EXTRACT(DAY FROM st.date - DATE '2024-12-29') / 7) * 7) AS week,
    sum(amount) amount
    from sum_transactions st 
    where type = 'income' and sales_id is not null 
    group by 1
    ),
    -- gross profit
    gross_profit as (
    select 
    date(DATE '2024-12-29' + INTERVAL '1 day' * FLOOR(EXTRACT(DAY FROM fs2.order_date - DATE '2024-12-29') / 7) * 7) AS week,
    sum((fs2.price * tp.profit  / 100) * fs2.quantity)  as amount
    from fact_sales fs2 
    left join tbl_product tp 
    on tp.id  = fs2.product_id 
    group by 1
    ),
    -- net_profit
    net_profit as (
    select 
    gp.week,
    (gp.amount - o.amount) as amount
    from gross_profit gp
    left join outcome o
    on  o.week = gp.week
    )
    select 
    u.week,
    u.unit_sold,
    g.amount as gmv,
    o.amount as outcome,
    r.amount as revenue,
    gp.amount as gross_profit,
    np.amount as nett_profit
    from uss u
    left join gmv g
    on g.week = u.week
    left join outcome o
    on o.week = u.week
    left join revenue r
    on r.week = u.week
    left join gross_profit gp 
    on gp.week = u.week 
    left join net_profit np 
    on np.week = u.week
        """)
    return weekly_finance_performance
