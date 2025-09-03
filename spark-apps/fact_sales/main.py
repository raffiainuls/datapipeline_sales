print("DEBUG: main.py started")
import sys
print("DEBUG sys.path:", sys.path)
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os 
from clickhouse_driver import Client 
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from fact_sales.sql import process_fact_sales
from clickhouse_writer.ch_writer import ch_writer
from clickhouse_writer.clickhouse_writer import ClickHouseWriter


def main():
    spark = SparkSession.builder \
        .appName("fact_sales") \
        .getOrCreate()
    
    df_tbl_sales = spark.read.parquet("hdfs://namenode:8020/data/landing/tbl_sales/parquet")
    df_tbl_product = spark.read.parquet("hdfs://namenode:8020/data/landing/tbl_product/parquet")
    df_tbl_promotions = spark.read.parquet("hdfs://namenode:8020/data/landing/tbl_promotions/parquet")

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
    
    fact_sales = process_fact_sales(spark)
    fact_sales.printSchema()
    fact_sales.show(truncate=False, n=20)

    writer = ch_writer()
    writer.write_to_clickhouse(fact_sales, table_name="fact_sales", order_by_cols="id", mode="overwrite")

    # Write to HDFS parquet as well
    hdfs_path = "hdfs://namenode:8020/data/fact_sales/parquet"
    writer.write_to_hdfs(fact_sales, hdfs_path=hdfs_path, mode="overwrite")

    spark.stop()


if __name__ == "__main__":
    main()  
