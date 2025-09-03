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
        .appName("tbl_product") \
        .getOrCreate()
    
    tbl_product = spark.read.parquet("hdfs://namenode:8020/data/landing/tbl_product/parquet")

    print("PARQUET tbl_product")
    tbl_product.printSchema()
    tbl_product.show()

    writer = ch_writer()
    writer.write_to_clickhouse(tbl_product, table_name="tbl_product", order_by_cols="id", mode="overwrite")

    spark.stop()


if __name__ == "__main__":
    main()  
