from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.sensors.external_task_sensor import ExternalTaskMarker, ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('transform_redshift_dag', default_args=default_args, schedule_interval="@once",catchup=False)


# Define the Glue Job
transform_task = GlueJobOperator(
    task_id='transform_task',
    job_name='glue_transform_task',
    script_location='https://greenbucketdata.s3.eu-west-2.amazonaws.com/python+scripts/transform.py',
    s3_bucket='https://greenbucketdata.s3.eu-west-2.amazonaws.com/glue/',  # S3 bucket where logs and local etl script will be uploaded
    aws_conn_id='aws_connection',  # Set up an AWS connection in Airflow
    region_name="eu-west-2",
    iam_role_name='dataPipeline-RedshiftIamRole-1CRXYWRMCF5KC',
    create_job_kwargs ={"GlueVersion": "4.0", "NumberOfWorkers": 4, "WorkerType": "G.1X", "Connections":{"Connections":["redshift-demo-connection"]},},
    dag=dag,
)

    
# Set task dependencies
#get_api >> transform_task
