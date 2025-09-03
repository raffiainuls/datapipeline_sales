import airflow 
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta


dag = DAG(
    dag_id = "static_datapipeline",
    default_args = {
        "owner": "Raffi",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    # schedule_interval = "@daily"
)
start = PythonOperator(
    task_id= "start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

## job tbl_branch
tbl_branch = SparkSubmitOperator(
    task_id = "tbl_branch",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/tbl_branch/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)

##  job tbl_customers 
tbl_customers = SparkSubmitOperator(
    task_id = "tbl_customers",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/tbl_customers/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)


##  job tbl_employee
tbl_employee = SparkSubmitOperator(
    task_id = "tbl_employee",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/tbl_employee/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)


##  job tbl_order_status
tbl_order_status = SparkSubmitOperator(
    task_id = "tbl_order_status",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/tbl_order_status/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)


##  job tbl_payment_method
tbl_payment_method = SparkSubmitOperator(
    task_id = "tbl_payment_method",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/tbl_payment_method/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)


##  job tbl_payment_method
tbl_payment_method = SparkSubmitOperator(
    task_id = "tbl_payment_method",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/tbl_payment_method/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)


##  job tbl_payment_status
tbl_payment_status = SparkSubmitOperator(
    task_id = "tbl_payment_status",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/tbl_payment_status/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)


##  job tbl_product
tbl_product = SparkSubmitOperator(
    task_id = "tbl_product",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/tbl_product/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)


##  job tbl_promotions
tbl_promotions = SparkSubmitOperator(
    task_id = "tbl_promotions",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/tbl_promotions/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)


##  job tbl_schedulle_employee
tbl_schedulle_employee = SparkSubmitOperator(
    task_id = "tbl_schedulle_employee",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/tbl_schedulle_employee/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)


##  job tbl_shipping_status
tbl_shipping_status = SparkSubmitOperator(
    task_id = "tbl_shipping_status",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/tbl_shipping_status/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)




end = PythonOperator(
    task_id= "end",
    python_callable = lambda: print("Jobs Finished Successfully"),
    dag=dag
)

start >> tbl_branch >> tbl_customers >> tbl_employee >> tbl_order_status >> tbl_payment_method >> tbl_payment_status >> tbl_product >> tbl_promotions >> tbl_schedulle_employee >> tbl_shipping_status >> end