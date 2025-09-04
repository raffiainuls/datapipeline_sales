## Data pipeline Sales 

#### Overview 
This project is data pipeline project that leverange kafka, flink for realtime streaming data and Apache Spark and Airflow for orchestrating batch and scheduled workflows. the project generates synthetic sales data, this project using some data sources database, PostgreSQL, Mysql, Oracle, SQL Server, and MongoDB. There is Python Generates data that stream data into data sources. data from any data source will send into kafka with connector kafka connect to store at each kafka topics. There is flink job that will cleaning and merge data transactions for each data source into one topics. and then this topic will be sink into hadoop with parquet format. from hadoop there is some spark jobs that execute query for create table dimension and then sink into clickhouse database. at the end this project show dashbord sales that showing sales performance from table clickhouse.

#### Features
- Data Generation: Python script generates stream data into each data source
- Connector Kafka Connect: There is some connector from kafka connect that will be send data from each data source into each kafka topics.
- Cleaning and merge data: 
