from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    settings = EnvironmentSettings.in_streaming_mode()
    table_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Source table from Kafka (streaming)
    table_env.execute_sql("""
         CREATE TABLE tbl_sales_oracle (
            payload ROW<
                id string,
                product_id INT,
                customer_id INT,
                branch_id INT,
                quantity INT,
                payment_method INT,
                order_date STRING,
                order_status INT,
                payment_status FLOAT,
                shipping_status FLOAT,
                is_online_transaction BOOLEAN,
                delivery_fee INT,
                is_free_delivery_fee STRING,
                created_at STRING,
                modified_at STRING
            >
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sqlserver-source1.salesdb.dbo.tbl_sales',
            'properties.bootstrap.servers' = 'host.docker.internal:9093',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # Print sink table
    table_env.execute_sql("""
        CREATE TABLE print_sink2 (
            payload ROW<
                ID string,
                PRODUCT_ID INT,
                CUSTOMER_ID INT,
                BRANCH_ID INT,
                QUANTITY INT,
                PAYMENT_METHOD INT,
                ORDER_DATE BIGINT,
                ORDER_STATUS INT,
                PAYMENT_STATUS FLOAT,
                SHIPPING_STATUS FLOAT,
                IS_ONLINE_TRANSACTION BOOLEAN,
                DELIVERY_FEE INT,
                IS_FREE_DELIVERY_FEE STRING,
                CREATED_AT BIGINT,
                MODIFIED_AT BIGINT
            >
        ) WITH (
            'connector' = 'print'
        )
    """)

    table_env.execute_sql("""
        CREATE TABLE print_sink4 (
                ID string,
                PRODUCT_ID INT,
                CUSTOMER_ID INT,
                BRANCH_ID INT,
                QUANTITY INT,
                PAYMENT_METHOD INT,
                ORDER_DATE BIGINT,
                ORDER_STATUS INT,
                PAYMENT_STATUS FLOAT,
                SHIPPING_STATUS FLOAT,
                IS_ONLINE_TRANSACTION BOOLEAN,
                DELIVERY_FEE INT,
                IS_FREE_DELIVERY_FEE STRING,
                CREATED_AT BIGINT,
                MODIFIED_AT BIGINT
        ) WITH (
            'connector' = 'print'
        )
    """)

    table_env.execute_sql("""
        CREATE TABLE print_sink5 (
                id string
        ) WITH (
            'connector' = 'print'
        )
    """)

    # Insert from source Kafka to print sink
    table_env.execute_sql("""
    INSERT INTO print_sink5
    SELECT 
    payload.id
    from tbl_sales_oracle     
    """)

if __name__ == "__main__":
    main()
