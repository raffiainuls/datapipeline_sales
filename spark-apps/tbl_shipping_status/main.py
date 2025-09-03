print("DEBUG: main.py started")
import sys
print("DEBUG sys.path:", sys.path)
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os 
from clickhouse_driver import Client 
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from clickhouse_writer.ch_writer import ch_writer
from clickhouse_writer.clickhouse_writer import ClickHouseWriter


def main():
    spark = SparkSession.builder \
        .appName("tbl_shipping_status") \
        .getOrCreate()
    
    tbl_shipping_status = spark.read.parquet("hdfs://namenode:8020/data/landing/tbl_shipping_status/parquet")

    print("PARQUET tbl_shipping_status")
    tbl_shipping_status.printSchema()
    tbl_shipping_status.show()

    writer = ch_writer()
    writer.write_to_clickhouse(tbl_shipping_status, table_name="tbl_shipping_status", order_by_cols="id", mode="overwrite")

    spark.stop()


if __name__ == "__main__":
    main()  
