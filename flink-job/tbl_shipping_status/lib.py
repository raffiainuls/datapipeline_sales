from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.table import StreamTableEnvironment, EnvironmentSettings


def create_source_table(table_name:str, topic_name: str, bootstrap_servers:str):
    stmt = f"""
    CREATE TABLE {table_name} (
    payload ROW<
    id int,
    name string, 
    created_time string
    >
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{topic_name}',
        'properties.bootstrap.servers' = '{bootstrap_servers}',
        'properties.group.id'   = 'source-group-static-tbl_shipping_status',
        'format' = 'json',                        
        'scan.startup.mode' = 'earliest-offset',  --  Read from beginning
        'scan.bounded.mode' = 'latest-offset'     -- for batching mode (job will be stop if react latest-offset)
    )
    """ 
    print(stmt)
    return stmt 

def create_sink_table(table_name:str, file_path:str):
    stmt = f"""
    CREATE TABLE {table_name} (
        id int,
        name string, 
        created_time timestamp(3),
        PRIMARY KEY (id) NOT ENFORCED
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
        id int,
        name string, 
        created_time timestamp(3),
        PRIMARY KEY (id) NOT ENFORCED
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
        name,
        cast(created_time as timestamp)
    from 
    {source_table_name}
    """
    print(stmt)
    return stmt