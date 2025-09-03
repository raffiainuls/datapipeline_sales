from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.table import StreamTableEnvironment


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
        'scan.startup.mode' = 'earliest-offset'
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