from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.udf import udf
import json

def main():
    # Setup environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    settings = EnvironmentSettings.in_streaming_mode()
    table_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Register Kafka source table (payload masih dalam string)
    table_env.execute_sql("""
        CREATE TABLE raw_sales (
            `schema` ROW<type STRING, optional BOOLEAN, name STRING, version INT>,
            `payload` STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'mongodb-sorce.salesdb.tbl_sales',
            'properties.bootstrap.servers' = 'host.docker.internal:9093',
            'properties.group.id' = 'flink-sales-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # Define UDF untuk parse JSON di dalam payload
    @udf(result_type=DataTypes.ROW([
        DataTypes.FIELD("oid", DataTypes.STRING()),
        DataTypes.FIELD("id", DataTypes.STRING()),
        DataTypes.FIELD("product_id", DataTypes.INT()),
        DataTypes.FIELD("customer_id", DataTypes.INT()),
        DataTypes.FIELD("branch_id", DataTypes.INT()),
        DataTypes.FIELD("quantity", DataTypes.INT()),
        DataTypes.FIELD("payment_method", DataTypes.INT()),
        DataTypes.FIELD("order_date", DataTypes.STRING()),
        DataTypes.FIELD("order_status", DataTypes.INT()),
        DataTypes.FIELD("payment_status", DataTypes.STRING()),
        DataTypes.FIELD("shipping_status", DataTypes.STRING()),
        DataTypes.FIELD("is_online_transaction", DataTypes.BOOLEAN()),
        DataTypes.FIELD("delivery_fee", DataTypes.INT()),
        DataTypes.FIELD("is_free_delivery_fee", DataTypes.BOOLEAN()),
        DataTypes.FIELD("created_at", DataTypes.STRING()),
        DataTypes.FIELD("modified_at", DataTypes.STRING())
    ]))
    def parse_payload(payload_str: str):
        parsed = json.loads(payload_str)
        return (
            parsed["_id"]["$oid"],
            parsed["id"],
            parsed["product_id"],
            parsed["customer_id"],
            parsed["branch_id"],
            parsed["quantity"],
            parsed["payment_method"],
            parsed["order_date"],
            parsed["order_status"],
            str(parsed.get("payment_status")),
            str(parsed.get("shipping_status")),
            parsed["is_online_transaction"],
            parsed["delivery_fee"],
            parsed["is_free_delivery_fee"],
            parsed["created_at"],
            str(parsed.get("modified_at"))
        )

    # Register UDF
    table_env.create_temporary_function("parse_payload", parse_payload)

    # Apply UDF to transform table
    table_env.execute_sql("""
        CREATE VIEW parsed_table AS
        SELECT parse_payload(payload) AS row_data FROM raw_sales
    """)

    table_env.execute_sql("""
        CREATE VIEW final_parsed AS
        SELECT
            row_data.oid,
            row_data.id,
            row_data.product_id,
            row_data.customer_id,
            row_data.branch_id,
            row_data.quantity,
            row_data.payment_method,
            row_data.order_date,
            row_data.order_status,
            row_data.payment_status,
            row_data.shipping_status,
            row_data.is_online_transaction,
            row_data.delivery_fee,
            row_data.is_free_delivery_fee,
            row_data.created_at,
            row_data.modified_at
        FROM parsed_table
    """)

    # Register Kafka sink
    table_env.execute_sql("""
        CREATE TABLE kafka_sink (
            oid STRING,
            id STRING,
            product_id INT,
            customer_id INT,
            branch_id INT,
            quantity INT,
            payment_method INT,
            order_date STRING,
            order_status INT,
            payment_status STRING,
            shipping_status STRING,
            is_online_transaction BOOLEAN,
            delivery_fee INT,
            is_free_delivery_fee BOOLEAN,
            created_at STRING,
            modified_at STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'parsed_sales',
            'properties.bootstrap.servers' = 'host.docker.internal:9093',
            'format' = 'json'
        )
    """)

    # Insert with explicit CAST to avoid type mismatch
    table_env.execute_sql("""
        INSERT INTO kafka_sink
        SELECT
            CAST(oid AS STRING),
            CAST(id AS STRING),
            product_id,
            customer_id,
            branch_id,
            quantity,
            payment_method,
            order_date,
            order_status,
            CAST(payment_status AS STRING),
            CAST(shipping_status AS STRING),
            is_online_transaction,
            delivery_fee,
            is_free_delivery_fee,
            created_at,
            modified_at
        FROM final_parsed
    """)

if __name__ == '__main__':
    main()
