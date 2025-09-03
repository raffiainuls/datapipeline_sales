

def process_product_performance(spark):
    product_performance = spark.sql("""
        select 
        fs2.product_id,
        tp.product_name,
        sum(quantity) as jumlah_terjual 
        from fact_sales fs2 
        left join tbl_product tp 
        on tp.id  = fs2.product_id 
        group by 1,2""")
    return product_performance
