# AWS data pipeline using Airflow

AWS end-to-end data pipeline using the following stack:
1. Lambda
2. S3
3. Glue
4. Airflow
5. Python
6. Redshift
7. EventBridge

Here is a pipeline diagram:

![aws_data_pipeline_white](https://github.com/maulik-dk/energy-data-pipeline/assets/58858333/950ee57f-01af-4bf1-91b9-dde384c2ff19)

Data-flow:
1. The Lambda function pulls the data from the API and stores the CSV file in the S3 bucket. This function is triggered every night by EventBridge.
2. Apache Airflow will orchestrate the ETL job.
3. Glue job reads the CSV file, applies transformation and ingests the data in the Redshift data warehouse.
4. CloudWatch is used for ETL job logs and IAM roles for permission to access the various resources.

Notes:
1. Create a dags folder in the airflow environment bucket and save all dags there. Add the requirement file to the root folder of the bucket.
2. Add energy_etl_job file in the Glue asset bucket.
3. Transform files can be saved in the bucket with the data.
