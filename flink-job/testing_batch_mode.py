from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
import getpass
import os



def create_source_table(table_name:str, topic_name: str, bootstrap_servers:str):
    stmt = f"""
    CREATE TABLE {table_name} (
    payload ROW<
    id int,
    name string, 
    location string, 
    email string, 
    phone string, 
    created_time string, 
    modified_time string 
    >
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{topic_name}',
        'properties.bootstrap.servers' = '{bootstrap_servers}',
        'properties.group.id'   = 'source-group-static-tbl_branch',
        'format' = 'json',                        -- ✅ CORRECT format
        'scan.startup.mode' = 'earliest-offset',  -- ✅ Read from beginning
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
        location string, 
        email string, 
        phone string, 
        created_time string, 
        modified_time string,
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
        location string, 
        email string, 
        phone string, 
        created_time string, 
        modified_time string,
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
        location, 
        email, 
        phone, 
        created_time, 
        modified_time 
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

    print(">>> OS user:", getpass.getuser())
    print(">>> HADOOP_USER_NAME:", os.environ.get("HADOOP_USER_NAME"))

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    table_env = StreamTableEnvironment.create(stream_execution_environment=env)

    source_table = "tbl_branch_hadoop"
    sink_table = "hdfs_branch_sink"
    print_table = "print_branch_table"
    topic = "source-postgres.public.tbl_branch"
    bootstrap_servers = "host.docker.internal:9093"
    file_path = 'hdfs://namenode:8020/data/tbl_branch/parquet'


    table_env.execute_sql(create_source_table(source_table, topic, bootstrap_servers))
    table_env.execute_sql(create_sink_table(sink_table, file_path))
    table_env.execute_sql(create_print_table(print_table))

    statement_set = table_env.create_statement_set()
    statement_set.add_insert_sql(set_insert_sql(source_table, sink_table))
    statement_set.add_insert_sql(set_insert_sql(source_table, print_table))
    statement_set.execute().wait()


if __name__ == "__main__":
    main()