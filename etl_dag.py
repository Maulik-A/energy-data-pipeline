from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.s3_list import S3ListOperator
from airflow.providers.amazon.aws.operators.s3_to_local_filesystem import S3ToLocalFilesystemOperator
from airflow.sensors.external_task_sensor import ExternalTaskMarker, ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('transform_redshift_dag', default_args=default_args, schedule_interval="@once",catchup=False)


# Define the Glue Job
transform_task = GlueJobOperator(
    task_id='transform_task',
    job_name='etl',
    script_location='s3://aws-glue-assets-703537846147-eu-west-2/scripts/energy_etl_job.py',
    s3_bucket='s3://aws-glue-assets-703537846147-eu-west-2/',  # S3 bucket where logs and local etl script will be uploaded
    aws_conn_id='aws_connection',  # Set up an AWS connection in Airflow
    region_name="eu-west-2",
    iam_role_name='dataPipeline-RedshiftIamRole-1CRXYWRMCF5KC',
    create_job_kwargs ={"GlueVersion": "4.0", "NumberOfWorkers": 4, "WorkerType": "G.1X", "Connections":{"Connections":["redshift-demo-connection"]},},
    dag=dag,
)

#### fatching latest file ###

# Define the task to list objects in the S3 bucket
list_s3_objects_task = S3ListOperator(
    task_id='list_s3_objects_task',
    bucket_name=s3_bucket_name,
    prefix=s3_prefix,
    delimiter='/',
    aws_conn_id=s3_conn_id,
    do_xcom_push=True,  # Push the list of objects to XCom
    task_id='list_s3_objects_task',
    dag=dag,
)

# Define a Python function to select the latest file from the list
def select_latest_file(files):
    return max(files, key=lambda x: x['LastModified'])

# Define the task to download the latest CSV file to the local directory
download_latest_csv_task = S3ToLocalFilesystemOperator(
    task_id='download_latest_csv_task',
    bucket_name=s3_bucket_name,
    bucket_key="{{ ti.xcom_pull(task_ids='list_s3_objects_task', key='return_value') | select_latest_file }}",  # Use the output of list_s3_objects_task
    local_file=os.path.join(local_dir, 'latest.csv'),
    aws_conn_id=s3_conn_id,
    task_id='download_latest_csv_task',
    dag=dag,
)

# Set task dependencies
list_s3_objects_task >> download_latest_csv_task >> transform_task


if __name__ == "__main__":
    dag.cli()
