
def process_branch_performance(spark):
    branch_performance = spark.sql("""
        select 
        fs2.branch_id,
        tb.name as branch_name,
        sum(quantity) as total_sales,
        sum(amount) amount
        from fact_sales fs2 
        left join tbl_branch tb 
        on tb.id = fs2.branch_id 
        group by 1,2 """)
    return branch_performance