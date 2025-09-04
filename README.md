## Data pipeline Sales 

#### Overview 
This project is data pipeline project that leverange kafka, flink for realtime streaming data and Apache Spark and Airflow for orchestrating batch and scheduled workflows. the project generates synthetic sales data, this project using some data sources database, PostgreSQL, Mysql, Oracle, SQL Server, and MongoDB. There is Python Generates data that stream data into data sources. data from any data source will send into kafka with connector kafka connect to store at each kafka topics. There is flink job that will cleaning and merge data transactions for each data source into one topics. and then this topic will be sink into hadoop with parquet format. from hadoop there is some spark jobs that execute query for create table dimension and then sink into clickhouse database. at the end this project show dashbord sales that showing sales performance from table clickhouse.

#### Features
- Data Generation: Python script generates stream data into each data source
- Connector Kafka Connect: There is some connector from kafka connect that will be send data from each data source into each kafka topics.
- Cleaning and merge data: for the transaction table, there will be 5 flink jobs for each topic from the data source that will cleaning and make all data into one topic, namely the `transaction`.
- sink into hadoop: there is job flink that sink data in kafka topic into Hadoop with format parquet
- schedulling spark job: there is some spark job for execute query to  create query dimension table, this job will get data from hadoop and execute query for table dimension and then the table will be sink into clickhouse table
- Dashbord Power BI: In this project there is dashbord show KPI and performance sales, data source from clickhouse database

### Instalation & setup 
#### Prerequisites 
- Docker & docker compose
- Pyhton
- Kafka (In this project i use windows local kafka and in this repository dont show kafka configuration)

#### Steps 
1. Clone the repository
   ```bash
   https://github.com/raffiainuls/datapipeline-sales
   cd datapipeline-sales
2. Start all container with
   ```bash
   docker-compose up
  In this project there is some container use pre-build images you can check in docker-compose file
4. Wait until the all images pulling and all container ready
5. actually in this project having table transaction and table reference that is ready to use and i save into csv file, the data in directory `/database-source/` and in this directory also there python file that import the data into postgreSQL. this python file can run, just edit config.yaml in this directory 
6. After the data already in postgres now in root directory  there is python file `stream-mongo.py`,  `stream-mysql.py`,  `stream-oracle.py`, `stream-postgres.py`,  `stream-sqlserver.py` all python file if run will generate data transaction and send into each data source 
7. after all python stream alredy run corectly data data in each database will be steaming and in this moment we can post connector kafka to send data in each data source into kafka. configuration connector for  each data source already available in directory `/connector-source/`  you can post all connector with postman or with this command 
```bash
curl -X POST http://localhost:8083/connectors \
     -H "Content-Type: application/json" \
     -d @postgres-source.json
```
8. if all connector running correctly in kafka topics should be available some topics. and if topics already available in kafka we can run flink some flink job to cleaning all topic transaction and merge data transaction into one topic. all flink job already volume mapped to flink container. if we want to run flink job  we should go in container flink-jobmanager with
```bash
docker exec -it flink-jobmanager bash
```
and then running all every flink job to cleanning and merge data transaction  `mysql.py`, `oracle.py`, `mongodb.py`, `postgres.py`, and `sqlserver.py` use this command to running 
```bash
flink run -py /opt/flink/job/mysql.py
```
9. 

