from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings


env = StreamExecutionEnvironment.get_execution_environment()
settings = EnvironmentSettings.in_streaming_mode()
table_env = StreamTableEnvironment.create(env, environment_settings=settings)

# env.enable_checkpointing(5000)  # tiap 5 detik
env.enable_checkpointing(5000)  # tiap 5 detik

# table_env.get_config().get_configuration().set_string(
#     "pipeline.jars",
#     "file:///opt/flink/lib/woodstox-core-asl-4.4.1;"
#     "file:///opt/flink/lib/flink-table-planner_2.12-1.19.3;"
#     "file:///opt/flink/lib/flink-csv-2.0.0;"
#     "file:///opt/flink/lib/kafka-schema-registry-client-7.7.0.jar;"
#     "file:///opt/flink/lib/kafka-schema-registry-client-7.7.0.jar;"
#     "file:///opt/flink/lib/stax2-api-4.2.2.jar"
# )

# Baca dari Kafka
table_env.execute_sql("""
CREATE TABLE kafka_sales (
  id STRING,
  product_id INT,
  customer_id INT,
  branch_id INT,
  quantity INT,
  payment_method INT,
  order_date TIMESTAMP(3),
  order_status INT,
  payment_status INT,
  shipping_status INT,
  is_online_transaction BOOLEAN,
  delivery_fee INT,
  is_free_delivery_fee BOOLEAN,
  created_at TIMESTAMP(3),
  modified_at TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'transaction_tbl_sales',
  'properties.bootstrap.servers' = 'host.docker.internal:9093',
  'properties.group.id' = 'flink-consumer',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
)
""")


table_env.execute_sql("""
CREATE TABLE hdfs_output (
  id STRING,
  product_id INT,
  customer_id INT,
  branch_id INT,
  quantity INT,
  payment_method INT,
  order_date TIMESTAMP(3),
  order_status INT,
  payment_status INT,
  shipping_status INT,
  is_online_transaction BOOLEAN,
  delivery_fee INT,
  is_free_delivery_fee BOOLEAN,
  created_at TIMESTAMP(3),
  modified_at TIMESTAMP(3)
) WITH (
  'connector' = 'filesystem',
  'path' = 'hdfs://namenode:8020/user/flink/output/sales3',
  'format' = 'csv'
)
""")


# Proses insert
table_env.execute_sql("""
INSERT INTO hdfs_output
SELECT * FROM kafka_sales
""")
