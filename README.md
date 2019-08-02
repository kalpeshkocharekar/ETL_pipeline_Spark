# ETL_pipeline_Spark
Create an ETL pipeline using Spark to analyze data and store it into PostgreSQL

Here we will be running the spark job to extract data from the JSON files and transform into dataframes and store it into the database.
you will need a Postgresql up and running with username etl.
Just need to specify the same in the etl.py file

Architecture:
(https://github.com/kalpeshkocharekar/ETL_pipeline_Spark/blob/master/architecture.png)

Below are the file descriptions

[etl.py](https://github.com/kalpeshkocharekar/ETL_pipeline_Spark/blob/master/etl.py) - contains the code for complete ETL pipeline

[create_tables.py](https://github.com/kalpeshkocharekar/ETL_pipeline_Spark/blob/master/create_tables.py) - will create table to initialize the database and create tables

[sql_queries.py](https://github.com/kalpeshkocharekar/ETL_pipeline_Spark/blob/master/sql_queries.py) - contains the sql queries to create tables used by create_tables.py

[etl.ipynb](https://github.com/kalpeshkocharekar/ETL_pipeline_Spark/blob/master/etl.ipynb) - to learn the step by step execution of spark ETL pipeline

[jsondata](https://github.com/kalpeshkocharekar/ETL_pipeline_Spark/tree/master/jsondata) - containes the sample data to be processed

