from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FileSink, OutputFileConfig, SimpleStringEncoder
from pyflink.common import Types, Path
import json

def to_csv(record: dict) -> str:
    # Handle nulls and convert all to string
    def safe(val):
        if val is None:
            return ''
        if isinstance(val, bool):
            return str(val).lower()  # true/false
        return str(val)

    fields = [
        'id', 'product_id', 'customer_id', 'branch_id', 'quantity',
        'payment_method', 'order_date', 'order_status', 'payment_status',
        'shipping_status', 'is_online_transaction', 'delivery_fee',
        'is_free_delivery_fee', 'created_at', 'modified_at'
    ]
    return ','.join([safe(record.get(field)) for field in fields])

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_props = {
        'bootstrap.servers': 'host.docker.internal:9093',
        'group.id': 'flink-consumer-group'
    }

    kafka_consumer = FlinkKafkaConsumer(
        topics='transaction_tbl_sales',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    ds = env.add_source(kafka_consumer)

    parsed = ds.map(
        lambda x: json.loads(x),
        output_type=Types.MAP(Types.STRING(), Types.STRING())  # typeinfo tetap map
    ).map(
        to_csv,
        output_type=Types.STRING()
    )

    sink = FileSink \
        .for_row_format(
            Path("hdfs://namenode:9000/kafka-output"),
            SimpleStringEncoder("utf-8")) \
        .with_output_file_config(OutputFileConfig.builder()
                                 .with_part_prefix("part")
                                 .with_part_suffix(".csv")
                                 .build()) \
        .build()

    parsed.sink_to(sink)

    env.execute("Kafka JSON to HDFS CSV")

if __name__ == '__main__':
    main()
