## Project: Udacity's Data pipeline using Apache Airflow.
---

### Overview
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

---

### Main goals
- Create high grade data pipelines that are dynamic, built from reusable tasks, can be monitored, and allow easy backfills.
- Process json files which reside in a Amazon S3 Bucket.
- Create an ETL process in order to load the data into redshift tables, in a star-schema, allowing analytics team to make analysis and get insights from them.
- Provide a data quality check task.

---

### The DAG illustration:
![The DAG Illustration](/example-dag.png)

### Project files
**udac_example.py**: The main file. It runs the tasks in the DAG. It resides in dags folder.

**create_tables.sql**: It contains the queries to create the tables. It can be used directly on redshift query editor in order to create the tables. Otherwise, one can create another task in the dag to do so. The code was already provided but it commented in.

**sql_queries.py**: It provides the queries to run the ETL itself.

**stage_redshift.py**: It represents the Airflow operator responsible for accessing the S3 bucket, processes its json files, and loads their data into a staging table on Redshift.

**load_fact.py**: It loads the data from the staging table into a fact table on Redshift.

**load_dimension.py**: It loads the data from the staging table into a dimension table on Redshift.

**data_quality.py**: It is an Apache Airflow operator which checks the data quality by evaluating the results of sql query and the expected result. Both of them are parameters passed by user.

### Additional Info
Do not forget to create the appropriate connection on Airflow. It is necessary to create a connection to Redshift and Amazon Web Services (AWS) by providing the correct credentials.