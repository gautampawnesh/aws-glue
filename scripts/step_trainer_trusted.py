import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1712831406828 = glueContext.create_dynamic_frame.from_catalog(database="stedi-v1", table_name="customer_trusted_v2", transformation_ctx="AWSGlueDataCatalog_node1712831406828")

# Script generated for node Amazon S3
AmazonS3_node1712830387797 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://step-trainer-landing-v1"], "recurse": True}, transformation_ctx="AmazonS3_node1712830387797")

# Script generated for node Join
AmazonS3_node1712830387797DF = AmazonS3_node1712830387797.toDF()
AWSGlueDataCatalog_node1712831406828DF = AWSGlueDataCatalog_node1712831406828.toDF()
Join_node1712814281187 = DynamicFrame.fromDF(AmazonS3_node1712830387797DF.join(AWSGlueDataCatalog_node1712831406828DF, (AmazonS3_node1712830387797DF['serialnumber'] == AWSGlueDataCatalog_node1712831406828DF['serialnumber']), "leftsemi"), glueContext, "Join_node1712814281187")

# Script generated for node Amazon S3
AmazonS3_node1712814381901 = glueContext.getSink(path="s3://step-trainer-trusted", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1712814381901")
AmazonS3_node1712814381901.setCatalogInfo(catalogDatabase="stedi-v1",catalogTableName="step_trainer_trusted_v2")
AmazonS3_node1712814381901.setFormat("json")
AmazonS3_node1712814381901.writeFrame(Join_node1712814281187)
job.commit()