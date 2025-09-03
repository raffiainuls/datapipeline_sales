from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.table import StreamTableEnvironment
from helper.function import create_env_v2
from sink_hadoop.lib import create_print_table, create_sink_table, create_source_table,set_insert_sql


def main():
    source_table = "tbl_sales_hadoop"
    sink_table = "hdfs_sink"
    print_table = "print_table"
    topic = "transaction_tbl_sales"
    bootstrap_servers = "host.docker.internal:9093"
    file_path = 'hdfs://namenode:8020/data/landing/tbl_sales/parquet'
    table_env = create_env_v2()


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