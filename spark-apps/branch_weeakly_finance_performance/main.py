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
from sum_transactions.sql import process_sum_transactions
from branch_weeakly_finance_performance.sql import process_branch_weakly_finance_performance
from clickhouse_writer.ch_writer import ch_writer
from clickhouse_writer.clickhouse_writer import ClickHouseWriter


def main():
    spark = SparkSession.builder \
        .appName("branch_weeakly_finance_performance") \
        .getOrCreate()
    
    df_tbl_sales = spark.read.parquet("hdfs://namenode:8020/data/landing/tbl_sales/parquet")
    df_tbl_product = spark.read.parquet("hdfs://namenode:8020/data/landing/tbl_product/parquet")
    df_tbl_branch = spark.read.parquet("hdfs://namenode:8020/data/landing/tbl_branch/parquet")
    df_tbl_promotions = spark.read.parquet("hdfs://namenode:8020/data/landing/tbl_promotions/parquet")
    df_tbl_employee = spark.read.parquet("hdfs://namenode:8020/data/landing/tbl_employee/parquet")
    

    print("PARQUET TBL_SALES")
    df_tbl_sales.printSchema()
    df_tbl_sales.show()

    print("PARQUET TBL_PRODUCT")
    df_tbl_product.printSchema()
    df_tbl_product.show()


    print("PARQUET TBL_BRANCH")
    df_tbl_branch.printSchema()
    df_tbl_branch.show()

    print("PARQUET TBL_PROMOTIONS")
    df_tbl_promotions.printSchema()
    df_tbl_promotions.show()

    print("PARQUET TBL_EMPLOYEE")
    df_tbl_employee.printSchema()
    df_tbl_employee.show()

    # create temptable 
    df_tbl_sales.createOrReplaceTempView("tbl_sales")
    df_tbl_product.createOrReplaceTempView("tbl_product")
    df_tbl_branch.createOrReplaceTempView("tbl_branch")
    df_tbl_promotions.createOrReplaceTempView("tbl_promotions")
    df_tbl_employee.createOrReplaceTempView("tbl_employee")
    
    print("Execution fact_sales")
    fact_sales = process_fact_sales(spark)
    fact_sales.createOrReplaceTempView("fact_sales")
    fact_sales.printSchema()
    fact_sales.show(truncate=False, n=20)

    print("Execution sum_transactions")
    sum_transactions = process_sum_transactions(spark)
    sum_transactions.createOrReplaceTempView("sum_transactions")
    sum_transactions.printSchema()
    sum_transactions.show(truncate=False, n=20)

    print("Execution branch_weeakly_finance_performance")
    branch_weeakly_finance_performance = process_branch_weakly_finance_performance(spark)
    branch_weeakly_finance_performance.printSchema()
    branch_weeakly_finance_performance.show(truncate=False, n=20)

    writer = ch_writer()
    writer.write_to_clickhouse(branch_weeakly_finance_performance, table_name="branch_weeakly_finance_performance", order_by_cols="name, week", mode="overwrite")

    spark.stop()


if __name__ == "__main__":
    main()  
