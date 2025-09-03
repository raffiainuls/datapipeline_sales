

def process_monthly_branch_performance(spark):
    monthly_branch_performance = spark.sql("""
      --- monthly_branch_performance 
        select 
        fs2.branch_id,
        tb.name  as branch_name,
        date(date_trunc('month',fs2.order_date)) as bulan,
        sum(quantity) as total_sales,
        sum(amount) as amount
        from fact_sales fs2 
        left join tbl_branch tb 
        on tb.id = fs2.branch_id 
        group by 1,2,3""")
    return monthly_branch_performance
