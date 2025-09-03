from pyspark.sql import SparkSession

# Replace 9000 if you exposed 8020 instead
spark = SparkSession.builder \
    .appName("ReadParquetFromHDFS") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:8020") \
    .getOrCreate()

# Read the Parquet data
df = spark.read.parquet("hdfs://localhost:8020/user/flink/output/sales-parquet")

df.printSchema()
df.show()
