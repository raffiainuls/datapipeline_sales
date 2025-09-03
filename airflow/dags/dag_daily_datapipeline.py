import airflow 
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta


dag = DAG(
    dag_id = "daily_datapipeline",
    default_args = {
        "owner": "Raffi",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
)
start = PythonOperator(
    task_id= "start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

## job fact_sales
fact_sales = SparkSubmitOperator(
    task_id = "fact_sales",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/fact_sales/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)

##  job sum_transactions 
sum_transactions = SparkSubmitOperator(
    task_id = "sum_transactions",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/sum_transactions/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)


##  job branch_daily_finance_performance
branch_daily_finance_performance = SparkSubmitOperator(
    task_id = "branch_daily_finance_performance",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/branch_daily_finance_performance/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)


##  job branch_finance_performance
branch_finance_performance = SparkSubmitOperator(
    task_id = "branch_finance_performance",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/branch_finance_performance/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)


##  job branch_monthly_finance_performance
branch_monthly_finance_performance = SparkSubmitOperator(
    task_id = "branch_monthly_finance_performance",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/branch_monthly_finance_performance/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)


##  job branch_performance
branch_performance = SparkSubmitOperator(
    task_id = "branch_performance",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/branch_performance/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)


##  job branch_weeakly_finance_performance
branch_weeakly_finance_performance = SparkSubmitOperator(
    task_id = "branch_weeakly_finance_performance",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/branch_weeakly_finance_performance/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)


##  job customers_retention
customers_retention = SparkSubmitOperator(
    task_id = "customers_retention",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/customers_retention/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)


##  job daily_finance_performance
daily_finance_performance = SparkSubmitOperator(
    task_id = "daily_finance_performance",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/daily_finance_performance/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)


##  job fact_employee
fact_employee = SparkSubmitOperator(
    task_id = "fact_employee",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/fact_employee/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)


##  job finance_performance
finance_performance = SparkSubmitOperator(
    task_id = "finance_performance",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/finance_performance/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)


##  job monthly_branch_performance
monthly_branch_performance = SparkSubmitOperator(
    task_id = "monthly_branch_performance",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/monthly_branch_performance/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)


##  job monthly_finance_performance
monthly_finance_performance = SparkSubmitOperator(
    task_id = "monthly_finance_performance",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/monthly_finance_performance/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)


##  job product_performance
product_performance = SparkSubmitOperator(
    task_id = "product_performance",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/product_performance/main.py",
    verbose=True,
     conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1"
    },
    dag=dag
)


##  job weeakly_finance_performance
weeakly_finance_performance = SparkSubmitOperator(
    task_id = "weeakly_finance_performance",
    conn_id = "spark_conn",
    env_vars={   # 👈 Add your custom environment variables here
        "HADOOP_USER_NAME": "flink",
        "PYTHONPATH": "/opt/airflow/jobs",
        "SPARK_CLASSPATH": "/opt/spark/jars/*"
    },
     jars="/opt/spark/jars/*",
    application="jobs/weeakly_finance_performance/main.py",
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

start >> fact_sales >> sum_transactions >> branch_daily_finance_performance >> branch_finance_performance >> branch_monthly_finance_performance >> branch_performance >> branch_weeakly_finance_performance >> customers_retention >> daily_finance_performance >> fact_employee >> finance_performance >> monthly_branch_performance >> monthly_finance_performance >> product_performance >> weeakly_finance_performance >> end