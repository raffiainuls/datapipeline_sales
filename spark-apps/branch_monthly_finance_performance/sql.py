

def process_branch_montlhy_finance_performance(spark):
    branch_montlhy_finance_performance = spark.sql("""
       -- dim_branch_montlhy_finance_performance 
        --  unit_sold 
        with uss as (
        select 
        tb.name,
        date(date_trunc('month', fs2.order_date)) as month,
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
        date(date_trunc('month', fs2.order_date)) as month,
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
        date(date_trunc('month', st.date)) as month,
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
        date(date_trunc('month', st.date)) as month,
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
        date(date_trunc('month', fs2.order_date)) as month,
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
        gp.month,
        (gp.amount - o.amount) as amount
        from gross_profit gp
        left join outcome o
        on  gp.name = o.name and  o.month = gp.month
        )
        select 
        u.name,
        u.month,
        u.unit_sold,
        g.amount as gmv,
        o.amount as outcome,
        r.amount as revenue,
        gp.amount as gross_profit,
        np.amount as nett_profit
        from uss u
        left join gmv g
        on g.month = u.month and g.name = u.name
        left join outcome o
        on o.month = u.month and o.name = u.name
        left join revenue r
        on r.month = u.month and r.name = u.name
        left join gross_profit gp 
        on gp.month = u.month and gp.name = u.name
        left join net_profit np 
        on np.month = u.month and np.name = u.name""")
    return branch_montlhy_finance_performance
