

def process_branch_daily_finance_performance(spark):
    branch_daily_finance_performance = spark.sql("""
        --dims_branch_daily_finance_performance
        --  unit_sold 
        with uss as (
        select 
        tb.name, 
        date(fs2.order_date) as date,
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
        date(fs2.order_date) as date,
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
        date(st.date) as date,
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
        date(st.date) as date,
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
        date(fs2.order_date) as date,
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
        gp.date,
        (gp.amount - o.amount) as amount
        from gross_profit gp
        left join outcome o
        on  o.date = gp.date and o.name = gp.name 
        )
        select 
        u.name,
        u.date,
        u.unit_sold,
        g.amount as gmv,
        o.amount as outcome,
        r.amount as revenue,
        gp.amount as gross_profit,
        np.amount as nett_profit
        from uss u
        left join gmv g
        on g.date = u.date and g.name = u.name
        left join outcome o
        on o.date = u.date and o.name = u.name 
        left join revenue r
        on r.date = u.date and r.name = u.name 
        left join gross_profit gp 
        on gp.date = u.date  and gp.name = u.name 
        left join net_profit np 
        on np.date = u.date and np.name = u.name""")
    return branch_daily_finance_performance
