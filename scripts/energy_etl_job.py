import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://greenbucketdata/Data/"], "recurse": True},
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Change Schema
ChangeSchema_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("fuel", "string", "fuel", "string"),
        ("perc", "string", "perc", "string"),
        ("from", "string", "ts", "string"),
    ],
    transformation_ctx="ChangeSchema_node2",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node2,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-703537846147-eu-west-2/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "public.energy_data",
        "connectionName": "redshift-demo-connection",
        "preactions": "CREATE TABLE IF NOT EXISTS public.energy_data (fuel VARCHAR, perc VARCHAR, ts VARCHAR);",
    },
    transformation_ctx="AmazonRedshift_node3",
)

job.commit()
