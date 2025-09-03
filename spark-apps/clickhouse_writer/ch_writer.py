from .clickhouse_writer import ClickHouseWriter

def ch_writer():
    return ClickHouseWriter(
        host="clickhouse-server",
        http_port=8123,   # for Spark JDBC
        native_port=9000, # for clickhouse_driver
        user="default",
        password="",
        database="default"
    )