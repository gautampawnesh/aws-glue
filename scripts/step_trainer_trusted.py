import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1712830386436 = glueContext.create_dynamic_frame.from_catalog(database="stedi-v1", table_name="customer_trusted_v1", transformation_ctx="AWSGlueDataCatalog_node1712830386436")

# Script generated for node Amazon S3
AmazonS3_node1712830387797 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://step-trainer-landing-v1"], "recurse": True}, transformation_ctx="AmazonS3_node1712830387797")

# Script generated for node Join
Join_node1712814281187 = Join.apply(frame1=AWSGlueDataCatalog_node1712830386436, frame2=AmazonS3_node1712830387797, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="Join_node1712814281187")

# Script generated for node Amazon S3
AmazonS3_node1712814381901 = glueContext.write_dynamic_frame.from_options(frame=Join_node1712814281187, connection_type="s3", format="json", connection_options={"path": "s3://step-trainer-trusted", "partitionKeys": []}, transformation_ctx="AmazonS3_node1712814381901")

job.commit()