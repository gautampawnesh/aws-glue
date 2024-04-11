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
AmazonS3_node1712811898990 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://customer-trusted-v1"], "recurse": True},
    transformation_ctx="AmazonS3_node1712811898990",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1712811932389 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-v1",
    table_name="accelerometer_landing_v1",
    transformation_ctx="AWSGlueDataCatalog_node1712811932389",
)

# Script generated for node Join
AmazonS3_node1712811898990DF = AmazonS3_node1712811898990.toDF()
AWSGlueDataCatalog_node1712811932389DF = AWSGlueDataCatalog_node1712811932389.toDF()
Join_node1712811949816 = DynamicFrame.fromDF(
    AmazonS3_node1712811898990DF.join(
        AWSGlueDataCatalog_node1712811932389DF,
        (
            AmazonS3_node1712811898990DF["email"]
            == AWSGlueDataCatalog_node1712811932389DF["user"]
        ),
        "leftsemi",
    ),
    glueContext,
    "Join_node1712811949816",
)

# Script generated for node Drop Fields
DropFields_node1712811982022 = DropFields.apply(
    frame=Join_node1712811949816,
    paths=[],
    transformation_ctx="DropFields_node1712811982022",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1712812898392 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1712811982022,
    database="stedi-v1",
    table_name="customer_curated_v1",
    transformation_ctx="AWSGlueDataCatalog_node1712812898392",
)

job.commit()
