import os 
from clickhouse_driver import Client
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


class ClickHouseWriter:
    def __init__(self, host, http_port, native_port, user, password, database="default"):
        self.host = host
        self.http_port = http_port
        self.native_port = native_port
        self.user = user
        self.password = password
        self.database = database

        self.jdbc_url = f"jdbc:clickhouse://{host}:{http_port}/{database}"
        self.jdbc_properties = {
            "user": user,
            "password": password,
            "driver": "com.clickhouse.jdbc.ClickHouseDriver"
        }

        self.client = Client(
            host=host,
            port=native_port,
            user=user,
            password=password,
            database=database
        )

    def spark_schema_to_clickhouse(self, schema: StructType, order_by_cols=None) -> str:
        type_mapping = {
            "StringType": "String",
            "IntegerType": "Int32",
            "LongType": "Int64",
            "DoubleType": "Float64",
            "FloatType": "Float32",
            "BooleanType": "UInt8",
            "DateType": "Date",
            "TimestampType": "DateTime64(3)",
        }
        if order_by_cols is None:
            order_by_cols = []

        # Pastikan order_by_cols list
        if isinstance(order_by_cols, str):
            order_by_cols = [order_by_cols]

        columns = []
        for field in schema.fields:
            spark_type = type(field.dataType).__name__
            ch_type = type_mapping.get(spark_type, "String")

            # Kolom ORDER BY selalu NOT NULL
            if field.name in order_by_cols:
                columns.append(f"{field.name} {ch_type}")
            else:
                if field.nullable:
                    ch_type = f"Nullable({ch_type})"
                columns.append(f"{field.name} {ch_type}")

        return ",\n".join(columns)
    
    def create_table_if_not_exists(self, table_name:str, schema: StructType, order_by_cols, mode="append"):
        if not order_by_cols: 
            raise ValueError("Parameter 'order_by_cols' require")
        
        if isinstance(order_by_cols, str):
            order_by_cols = [col.strip() for col in order_by_cols.split(",")]

        # Check if table exists
        exists_query = f"EXISTS TABLE {self.database}.{table_name}"
        exists = self.client.execute(exists_query)[0][0]  # 1 if exists, 0 if not

        # If table exists and mode is overwrite → truncate
        if exists and mode == "overwrite":
            self.client.execute(f"TRUNCATE TABLE {self.database}.{table_name}")
            print(f"Table '{table_name}' truncated in ClickHouse.")

        
        order_by_str = ", ".join(order_by_cols)
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.database}.{table_name} (
            {self.spark_schema_to_clickhouse(schema, order_by_cols)}
        )
        ENGINE = MergeTree()
        ORDER BY ({order_by_str})
        """
        print("DDL Create Table")
        print(f"{ddl}")
        self.client.execute(ddl)
        print(f"Table '{table_name} successfull created in Clickhouse.'")

    def write_to_clickhouse(self, df:DataFrame, table_name:str, mode: str = "append", order_by_cols=None):
        self.create_table_if_not_exists(table_name, df.schema, order_by_cols, mode=mode)
        df.write \
            .mode("append") \
            .jdbc(self.jdbc_url, table_name, properties=self.jdbc_properties)   # mode pantex append because if overwrite already handle in method create table if not exist in above there function truncate table if mode overwrite 
        print(f"Data Succsessful save into Clickhouse: {table_name}")


    def write_to_hdfs(self, df: DataFrame, hdfs_path: str, mode: str = "overwrite"):
        try:
            df.write.mode(mode).parquet(hdfs_path)
            print(f"Data successfully saved to HDFS at: {hdfs_path}")
        except Exception as e:
            print(f"Failed to write data to HDFS: {e}")
            # tidak raise ulang agar program lanjut



        

