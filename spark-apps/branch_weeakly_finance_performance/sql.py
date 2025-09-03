

def process_branch_weakly_finance_performance(spark):
    branch_weakly_finance_performance = spark.sql("""
       -- dim_branch_weakly_finance_performance 
        --  unit_sold 
        with uss as (
        select 
        tb.name, 
        date(DATE '2024-12-29' + INTERVAL '1 day' * FLOOR(EXTRACT(DAY FROM fs2.order_date - DATE '2024-12-29') / 7) * 7) AS week,
        sum(quantity) unit_sold
        from fact_sales fs2 
        left join tbl_branch tb
        on tb.id = fs2.branch_id 
        group by 1,2
        ),
        -- gmv 
        gmv as (
        select 
        tb.name, 
        date(DATE '2024-12-29' + INTERVAL '1 day' * FLOOR(EXTRACT(DAY FROM fs2.order_date - DATE '2024-12-29') / 7) * 7) AS week,
        sum(quantity * price) amount
        from fact_sales fs2 
        left join tbl_branch tb 
        on tb.id = fs2.branch_id 
        group by 1,2
        ),
        -- total outcome
        outcome as (
        select 
        tb.name, 
        date(DATE '2024-12-29' + INTERVAL '1 day' * FLOOR(EXTRACT(DAY FROM st.date - DATE '2024-12-29') / 7) * 7) AS week,
        sum(amount) amount
        from sum_transactions st 
        left join tbl_branch tb
        on tb.id = st.branch_id
        where type = 'outcome'
        group by 1,2
        ),
        -- revenue 
        revenue as (
        select
        tb.name, 
        date(DATE '2024-12-29' + INTERVAL '1 day' * FLOOR(EXTRACT(DAY FROM st.date - DATE '2024-12-29') / 7) * 7) AS week,
        sum(amount) amount
        from sum_transactions st 
        left join tbl_branch tb
        on tb.id = st.branch_id 
        where type = 'income' and sales_id is not null 
        group by 1,2 
        ),
        -- gross profit
        gross_profit as (
        select 
        tb.name, 
        date(DATE '2024-12-29' + INTERVAL '1 day' * FLOOR(EXTRACT(DAY FROM fs2.order_date - DATE '2024-12-29') / 7) * 7) AS week,
        sum((fs2.price * tp.profit  / 100) * fs2.quantity)  as amount
        from fact_sales fs2 
        left join tbl_product tp 
        on tp.id  = fs2.product_id 
        left join tbl_branch tb
        on tb.id = fs2.branch_id 
        group by 1,2 
        ),
        -- net_profit
        net_profit as (
        select 
        gp.name, 
        gp.week,
        (gp.amount - o.amount) as amount
        from gross_profit gp
        left join outcome o
        on  o.week = gp.week and o.name = gp.name
        )
        select 
        u.name, 
        u.week,
        u.unit_sold,
        g.amount as gmv,
        o.amount as outcome,
        r.amount as revenue,
        gp.amount as gross_profit,
        np.amount as nett_profit
        from uss u
        left join gmv g
        on g.week = u.week and g.name = u.name 
        left join outcome o
        on o.week = u.week and o.name = u.name
        left join revenue r
        on r.week = u.week and r.name = u.name
        left join gross_profit gp 
        on gp.week = u.week  and gp.name = u.name
        left join net_profit np 
        on np.week = u.week and np.name = u.name """)
    return branch_weakly_finance_performance
