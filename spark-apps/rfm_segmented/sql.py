def process_rfm_segmented(spark):
    rfm_segmented = spark.sql("""
        with data as (
            select 
                f.id as id,
                t.id as customer_id,
                t.name as name,
                f.order_date,
                f.amount
            from fact_sales f
            left join tbl_customers t 
                on f.customer_id = t.id
            where to_date(f.order_date) >= current_date()
        ),
        rfm as (
            select 
                customer_id,
                name,
                max(order_date) as last_order,
                count(id) as frequency,
                sum(amount) as monetary
            from data 
            group by customer_id, name 
        ),
        score as (
            select 
                customer_id,
                name,
                datediff(current_date(), to_date(last_order)) as recency,
                frequency,
                monetary,
                to_date(last_order) as last_order
            from rfm 
        ),
        avg_value as (
            select
                ceil(avg(frequency)) as avg_frequency, 
                ceil(avg(monetary)) as avg_monetary
            from score
        ),
        result as( 
            select 
                customer_id,
                name, 
                recency,
                frequency,
                monetary,
                last_order,
                case 
                    when recency <= 30 and frequency >= av.avg_frequency and monetary >= av.avg_monetary then 'Perfect Customers'
                    when frequency >= av.avg_frequency and  monetary >= av.avg_monetary then 'Good Customers'
                    when frequency >= av.avg_frequency and recency >= 90 then 'Loyal But inactive'
                    when frequency >= av.avg_frequency then 'Loyal Customers'
                    when monetary >= av.avg_monetary then 'Big Spenders'
                    when recency <= 30 and frequency = 1 then 'New Customers'
                    else 'Just Customers'
                end as segment
            from score cross join avg_value av
        ) 
        select *
        from result
    """)
    return rfm_segmented
