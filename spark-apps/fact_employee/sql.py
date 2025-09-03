def process_fact_employee(spark):
    fact_employee = spark.sql("SELECT * FROM tbl_employee where active = true")
    return fact_employee