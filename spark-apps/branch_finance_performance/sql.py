
def process_branch_finance_performance(spark):
    branch_finance_performance = spark.sql("""
        -- unit_sold  
        with uss as (
        select 
        tb.name,
        sum(quantity) unit_sold
        from fact_sales fs2 
        left join tbl_branch tb 
        on tb.id = fs2.branch_id
        group by 1
        ),
        -- gmv 
        gmv as (
        select 
        tb.name,
        sum(quantity * price) amount
        from fact_sales fs2 
        left join tbl_branch tb
        on tb.id = fs2.branch_id
        group by 1
        ),
        -- total outcome
        outcome as (
        select 
        tb.name,
        sum(amount) amount
        from sum_transactions st 
        left join tbl_branch tb 
        on tb.id = st.branch_id
        where type = 'outcome'
        group by 1
        ),
        -- revenue 
        revenue as (
        select
        tb.name,
        sum(amount) amount
        from sum_transactions st 
        left join tbl_branch tb 
        on tb.id = st.branch_id
        where type = 'income' and sales_id is not null 
        group by 1
        ),
        -- gross profit
        gross_profit as (
        select 
        tb.name, 
        sum((fs2.price * tp.profit  / 100) * fs2.quantity)  as amount
        from fact_sales fs2 
        left join tbl_product tp 
        on tp.id  = fs2.product_id 
        left join tbl_branch tb 
        on tb.id = fs2.branch_id 
        group by 1
        ),
        -- net_profit
        net_profit as (
        select 
        u.name,
        (gp.amount - o.amount) as amount
        from uss u
        left join outcome o
        on o.name = u.name
        left join gmv g 
        on g.name = u.name 
        left join revenue r 
        on r.name = u.name 
        left join gross_profit gp 
        on gp.name = u.name
        )
        SELECT 
        u.name, 
        o.amount as outcome, 
        g.amount as gmv, 
        r.amount as revenue, 
        gp.amount as gross_profit, 
        np.amount as net_profit
        from uss u
        left join outcome o
        on o.name = u.name
        left join gmv g 
        on g.name = u.name 
        left join revenue r 
        on r.name = u.name 
        left join gross_profit gp 
        on gp.name = u.name
        left join net_profit np 
        on np.name = u.name """)
    return branch_finance_performance