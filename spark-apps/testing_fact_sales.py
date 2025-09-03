from pyspark.sql import SparkSession
from dotenv import load_dotenv 
import os 
from clickhouse_driver import Client
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


class ClickHouseWriter:
    def __init__(self, host, http_port, native_port, user, password, database="default"):
        self.host = host
        self.http_port = http_port      # for Spark JDBC
        self.native_port = native_port  # for clickhouse_driver
        self.user = user
        self.password = password
        self.database = database

        # JDBC for Spark → HTTP port
        self.jdbc_url = f"jdbc:clickhouse://{host}:{http_port}/{database}"
        self.jdbc_properties = {
            "user": user,
            "password": password,
            "driver": "com.clickhouse.jdbc.ClickHouseDriver"
        }

        # Native client → native port
        self.client = Client(
            host=host,
            port=native_port,
            user=user,
            password=password,
            database=database
        )

    def spark_schema_to_clickhouse(self, schema: StructType) -> str:
        type_mapping = {
            "StringType": "String",
            "IntegerType": "Int32",
            "LongType": "Int64",
            "DoubleType": "Float64",
            "FloatType": "Float32",
            "BooleanType": "UInt8",
            "DateType": "Date",
            "TimestampType": "DateTime",
        }

        columns = []
        for field in schema.fields:
            spark_type = type(field.dataType).__name__
            ch_type = type_mapping.get(spark_type, "String")
            if field.nullable:
                ch_type = f"Nullable({ch_type})"
            columns.append(f"{field.name} {ch_type}")
        return ",\n".join(columns)

    def create_table_if_not_exists(self, table_name: str, schema: StructType, order_by="id"):
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.database}.{table_name} (
            {self.spark_schema_to_clickhouse(schema)}
        )
        ENGINE = MergeTree()
        ORDER BY {order_by}
        """
        try:
            self.client.execute(ddl)
            print(f"Tabel '{table_name}' berhasil dicek/dibuat di ClickHouse.")
        except Exception as e:
            print("Gagal membuat tabel:", e)
            raise

    def write_to_clickhouse(self, df: DataFrame, table_name: str, mode: str = "append", order_by="id"):
        self.create_table_if_not_exists(table_name, df.schema, order_by)
        df.write \
            .mode(mode) \
            .jdbc(self.jdbc_url, table_name, properties=self.jdbc_properties)
        print(f"Data berhasil disimpan ke ClickHouse: {table_name}")



load_dotenv()

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_TABLE = os.getenv("CLICKHOUSE_TABLE")
HDFS_PARQUET_PATH = os.getenv("HDFS_PARQUET_PATH")

print("DEBUGGING")
print(f"DEBUG HOST ENV {CLICKHOUSE_HOST}")
print(f"DEBUG PORT ENF {CLICKHOUSE_PORT}")
print(f"DEBUG USER ENV {CLICKHOUSE_USER}")
print(f"DEBUG PASSWORD {CLICKHOUSE_PASSWORD}")

spark = SparkSession.builder \
    .appName("Fack_sales") \
    .getOrCreate()

# read parquet 
df_tbl_sales = spark.read.parquet("hdfs://namenode:8020/data/tbl_sales/parquet")
df_tbl_product = spark.read.parquet("hdfs://namenode:8020/data/tbl_product/parquet")
df_tbl_promotions = spark.read.parquet("hdfs://namenode:8020/data/tbl_promotions/parquet")


print("PARQUET TBL_SALES")
df_tbl_sales.printSchema()
df_tbl_sales.show()

print("PARQUET TBL_PRODUCT")
df_tbl_product.printSchema()
df_tbl_product.show()


print("PARQUET TBL_PROMOTIONS")
df_tbl_promotions.printSchema()
df_tbl_promotions.show()

# create temptable 

df_tbl_sales.createOrReplaceTempView("tbl_sales")
df_tbl_product.createOrReplaceTempView("tbl_product")
df_tbl_promotions.createOrReplaceTempView("tbl_promotions")


fact_sales = spark.sql("""
        SELECT
        ts.id as id,
        ts.product_id as product_id,
        ts.customer_id,
        ts.branch_id,
        ts.quantity,
        ts.payment_method,
        ts.order_date,
        ts.order_status,
        ts.payment_status,
        ts.shipping_status,
        ts.is_online_transaction,
        ts.delivery_fee,
        ts.is_free_delivery_fee,
        ts.created_at,
        ts.modified_at,
        tp.product_name,
        tp.category AS product_category,
        tp.sub_category AS sub_category_product,
        tp.price,
        tps.disc,
        tps.event_name AS disc_name,
        CAST(
            CASE
                WHEN tps.disc IS NOT NULL
                THEN (tp.price * ts.quantity) - (tp.price * ts.quantity) * COALESCE(tps.disc, 0) / 100
                ELSE tp.price * ts.quantity
            END AS BIGINT
        ) AS amount
    FROM tbl_sales ts
    LEFT JOIN tbl_product tp
        ON tp.id = ts.product_id
    LEFT JOIN tbl_promotions tps
        ON to_date(ts.order_date) = tps.time
    WHERE ts.order_status = 2
        AND ts.payment_status = 2
        AND (ts.shipping_status = 2 OR ts.shipping_status IS NULL) 
        """)

fact_sales.printSchema()  # Menampilkan struktur kolom
fact_sales.show(truncate=False, n=20)  # Menampilkan 20 baris tanpa memotong kolom


# jdbc_url = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/default?format=JSONEachRow"

# # 🔹 Definisikan tipe kolom + nullable sesuai kebutuhan (harus urut sesuai schema DataFrame!)
# clickhouse_column_types = """
# id String,
# product_id Int32,
# customer_id Int32,
# branch_id Int32,
# quantity Int32,
# payment_method String,
# order_date DateTime,
# order_status Nullable Int32,
# payment_status Nullable Int32,
# shipping_status Nullable Int32,
# is_online_transaction Nullable UInt8,
# delivery_fee Nullable Float64,
# is_free_delivery_fee Nullable UInt8,
# created_at DateTime,
# modified_at Nullable DateTime,
# product_name String,
# product_category String,
# sub_category_product Nullable String,
# price Float64,
# disc Nullable Float32,
# disc_name Nullable String,
# amount Int64
# """





# fact_sales.write \
#     .format("jdbc") \
#     .option("url", jdbc_url) \
#     .option("dbtable", CLICKHOUSE_TABLE) \
#     .option("user", CLICKHOUSE_USER) \
#     .option("password", CLICKHOUSE_PASSWORD) \
#     .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
#     .option("createTableOptions", "ENGINE = MergeTree() ORDER BY id") \
#     .mode("append") \
#     .save()


writer = ClickHouseWriter(
    host="clickhouse-server",
    http_port=8123,   # for Spark JDBC
    native_port=9000, # for clickhouse_driver
    user="default",
    password="",
    database="default"
)
fact_sales.printSchema()
writer.write_to_clickhouse(fact_sales, table_name="fact_sales")


spark.stop()
