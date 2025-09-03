
def process_customers_retention(spark):
    customers_retention = spark.sql("""
        -- monthly custome retention 
        WITH monthly_customers AS (
            SELECT 
                customer_id,
                DATE_TRUNC('month', order_date) AS bulan
            FROM fact_sales
            GROUP BY customer_id, bulan
        ),
        retention AS (
            SELECT 
                mc1.bulan,
                COUNT(DISTINCT mc1.customer_id) AS total_customers,
                COUNT(DISTINCT mc2.customer_id) AS retained_customers
            FROM monthly_customers mc1
            LEFT JOIN monthly_customers mc2
                ON mc1.customer_id = mc2.customer_id  
                AND mc2.bulan = mc1.bulan + INTERVAL '1 month'
            GROUP BY mc1.bulan
        ),
        retention_rate AS (
            SELECT 
                bulan,
                total_customers,
                retained_customers, 
                (retained_customers * 100.0 / NULLIF(total_customers, 0)) AS retention_rate_percent
            FROM retention
        )
        SELECT * FROM retention_rate """)
    return customers_retention