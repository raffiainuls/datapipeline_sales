print("DEBUG: main.py started")
import sys
print("DEBUG sys.path:", sys.path)
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os 
from clickhouse_driver import Client 
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from fact_employee.sql import process_fact_employee
from clickhouse_writer.ch_writer import ch_writer
from clickhouse_writer.clickhouse_writer import ClickHouseWriter



def main():
    spark = SparkSession.builder \
        .appName("fact_employee") \
        .getOrCreate()
    
    df_fact_employee = spark.read.parquet("hdfs://namenode:8020/data/landing/tbl_employee/parquet")


    print("PARQUET TBL_EMPLOYEE")
    df_fact_employee.printSchema()
    df_fact_employee.show()


    # create temptable 
    df_fact_employee.createOrReplaceTempView("tbl_employee")

    
    print("Execution fact_employee")
    fact_employee = process_fact_employee(spark)
    fact_employee.createOrReplaceTempView("fact_employee")
    fact_employee.printSchema()
    fact_employee.show(truncate=False, n=20)


    writer = ch_writer()
    writer.write_to_clickhouse(fact_employee, table_name="fact_employee", order_by_cols="id", mode="overwrite")

    spark.stop()


if __name__ == "__main__":
    main()  
