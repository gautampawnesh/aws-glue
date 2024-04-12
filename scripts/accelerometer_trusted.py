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
AWSGlueDataCatalog_node1712810832560 = glueContext.create_dynamic_frame.from_catalog(database="stedi-v1", table_name="accelerometer_landing_v1", transformation_ctx="AWSGlueDataCatalog_node1712810832560")

# Script generated for node Amazon S3
AmazonS3_node1712810848109 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://customer-trusted-v1"], "recurse": True}, transformation_ctx="AmazonS3_node1712810848109")

# Script generated for node Join
Join_node1712810873072 = Join.apply(frame1=AmazonS3_node1712810848109, frame2=AWSGlueDataCatalog_node1712810832560, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1712810873072")

# Script generated for node Drop Fields
DropFields_node1712811006500 = DropFields.apply(frame=Join_node1712810873072, paths=["email", "phone"], transformation_ctx="DropFields_node1712811006500")

# Script generated for node Amazon S3
AmazonS3_node1712811257708 = glueContext.getSink(path="s3://accelerometer-trusted-v1", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1712811257708")
AmazonS3_node1712811257708.setCatalogInfo(catalogDatabase="stedi-v1",catalogTableName="accelerometer_trusted_v2")
AmazonS3_node1712811257708.setFormat("json")
AmazonS3_node1712811257708.writeFrame(DropFields_node1712811006500)
job.commit()