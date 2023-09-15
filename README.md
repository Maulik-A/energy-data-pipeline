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
1. Lambda function pulls the data from the API and stores the CSV file in the S3 bucket. This function is triggered everynight by EventBridge.
2. Apache Airflow will orchestrate the ETL job.
3. Glue job reads the CSV file, apply transformation and ingest the data in Redshift data warehouse.
4. CloudWatch is used for ETL job logs and IAM roles for permission to access the various resources.
