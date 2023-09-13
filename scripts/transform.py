import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
  

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
df = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://greenbucketdata.s3.eu-west-2.amazonaws.com/Data/2023-08-13-09.csv"],
        "recurse": True,
    },
    transformation_ctx="df",
)

# Script generated for node Change Schema
changeschema_df = ApplyMapping.apply(
    frame=df,
    mappings=[
        ("fuel", "string", "fuel", "string"),
        ("perc", "string", "perc", "string"),
        ("from", "string", "from", "string"),
        ("to", "string", "to", "string"),
    ],
    transformation_ctx="changeschema_df",
)


redshift_output = glueContext.write_dynamic_frame.from_options(
    frame=changeschema_df,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://datapipeline-redshifttempdatabucket-155is2x1xyfwl/",
        "useConnectionProperties": "true",
        "aws_iam_role": "arn:aws:iam::703537846147:role/dataPipeline-RedshiftIamRole-1CRXYWRMCF5KC",
        "dbtable": "public.energy_data",
        "connectionName": "redshift-demo-connection",
        "preactions": "DROP TABLE IF EXISTS public.energy_data; CREATE TABLE IF NOT EXISTS public.energy_data (fuel VARCHAR, perc VARCHAR, from VARCHAR, to VARCHAR);",
    },
    transformation_ctx="redshift_output",
)

job.commit()
