import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1712827879610 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-v1",
    table_name="step_trainer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1712827879610",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1712827890624 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-v1",
    table_name="accelerometer_trusted_v1",
    transformation_ctx="AWSGlueDataCatalog_node1712827890624",
)

# Script generated for node Join
Join_node1712827906770 = Join.apply(
    frame1=AWSGlueDataCatalog_node1712827890624,
    frame2=AWSGlueDataCatalog_node1712827879610,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1712827906770",
)

# Script generated for node Amazon S3
AmazonS3_node1712828071022 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1712827906770,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://machine-learning-curated", "partitionKeys": []},
    transformation_ctx="AmazonS3_node1712828071022",
)

job.commit()
