from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.table import StreamTableEnvironment
import getpass
import os

def create_source_table(table_name: str, topic_name: str, bootstrap_servers: str):
    stmt = f""" 
    CREATE TABLE {table_name} (
        id                      VARCHAR,
        product_id              INT, 
        customer_id             INT, 
        branch_id               INT, 
        quantity                INT, 
        payment_method          INT, 
        order_date              TIMESTAMP(3),
        order_status            INT, 
        payment_status          INT,
        shipping_status         INT,
        is_online_transaction   BOOLEAN,
        delivery_fee            INT,
        is_free_delivery_fee    BOOLEAN,
        created_at              TIMESTAMP(3),
        modified_at             TIMESTAMP(3)
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{topic_name}',
        'properties.bootstrap.servers' = '{bootstrap_servers}',
        'properties.group.id' = 'source-group',
        'format' = 'json',
        'scan.startup.mode' = 'latest-offset'
    )
    """
    print(stmt)
    return stmt

def create_sink_table(table_name:str, file_path:str):
    stmt = f"""
    CREATE TABLE {table_name} (
        id                      VARCHAR,
        product_id              INT, 
        customer_id             INT, 
        branch_id               INT, 
        quantity                INT, 
        payment_method          INT, 
        order_date              TIMESTAMP(3),
        order_status            INT, 
        payment_status          INT,
        shipping_status         INT,
        is_online_transaction   BOOLEAN,
        delivery_fee            INT,
        is_free_delivery_fee    BOOLEAN,
        created_at              TIMESTAMP(3),
        modified_at             TIMESTAMP(3)
    ) WITH (
      'connector' = 'filesystem',
      'path' = '{file_path}',
      'format' = 'parquet'
    )
    """
    print(stmt)
    return stmt 

def create_print_table(table_name:str):
    stmt = f"""
    CREATE TABLE {table_name}(
        id                      VARCHAR,
        product_id              INT, 
        customer_id             INT, 
        branch_id               INT, 
        quantity                INT, 
        payment_method          INT, 
        order_date              TIMESTAMP(3),
        order_status            INT, 
        payment_status          INT,
        shipping_status         INT,
        is_online_transaction   BOOLEAN,
        delivery_fee            INT,
        is_free_delivery_fee    BOOLEAN,
        created_at              TIMESTAMP(3),
        modified_at             TIMESTAMP(3)
    ) WITH (
        'connector'= 'print'
    )
    """
    print(stmt)
    return stmt

def set_insert_sql(source_table_name:str, sink_table_name:str):
    stmt = f"""
    INSERT INTO {sink_table_name}
    select 
    id,
    product_id,
    customer_id,
    branch_id,
    quantity,
    payment_method,
    order_date,
    order_status,
    payment_status,
    shipping_status,
    is_online_transaction,
    delivery_fee,
    is_free_delivery_fee,
    created_at, 
    modified_at
    from 
    {source_table_name}
    """
    print(stmt)
    return stmt


def main():
    current_user = getpass.getuser()
    print(f"📌 Python job is running as user: {current_user}")

    # Optional tambahan info
    print(f"📁 Current working dir: {os.getcwd()}")
    print(f"📂 Home dir: {os.path.expanduser('~')}")
    print(f"💡 UID: {os.getuid()}")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.enable_checkpointing(6000)
    table_env = StreamTableEnvironment.create(stream_execution_environment=env)

    # table_env.get_config().get_configuration().set_string(
    #     "pipeline.jars",
    #     "file:///opt/flink/lib/flink-parquet-1.19.3.jar;file:///opt/flink/lib/parquet-hadoop-1.15.2.jar;file:///opt/flink/lib/parquet-column-1.15.2.jar;file:///opt/flink/lib/parquet-common-1.15.2.jar;file:///opt/flink/lib/parquet-avro-1.15.2.jar;file:///opt/flink/lib/snappy-java-1.1.10.7.jar"
    # )

    source_table = "tbl_sales"
    sink_table = "hdfs_sink"
    print_table = "print_table"
    topic = "transaction_tbl_sales"
    bootstrap_servers = "host.docker.internal:9093"
    file_path = 'hdfs://namenode:8020/user/flink/output/sales-parquet'

    table_env.execute_sql(
        create_source_table(
            source_table, topic, bootstrap_servers
          )
    )

    table_env.execute_sql(create_sink_table(sink_table, file_path))
    table_env.execute_sql(create_print_table(print_table))

    statement_set = table_env.create_statement_set()
    statement_set.add_insert_sql(set_insert_sql(source_table, sink_table))
    statement_set.add_insert_sql(set_insert_sql(source_table,print_table))
    statement_set.execute().wait()


if __name__ == "__main__":
    main()



    

