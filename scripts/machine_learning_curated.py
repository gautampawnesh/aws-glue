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
AWSGlueDataCatalog_node1712827879610 = glueContext.create_dynamic_frame.from_catalog(database="stedi-v1", table_name="step_trainer_trusted_v2", transformation_ctx="AWSGlueDataCatalog_node1712827879610")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1712827890624 = glueContext.create_dynamic_frame.from_catalog(database="stedi-v1", table_name="accelerometer_trusted_v2", transformation_ctx="AWSGlueDataCatalog_node1712827890624")

# Script generated for node Join
AWSGlueDataCatalog_node1712827890624DF = AWSGlueDataCatalog_node1712827890624.toDF()
AWSGlueDataCatalog_node1712827879610DF = AWSGlueDataCatalog_node1712827879610.toDF()
Join_node1712827906770 = DynamicFrame.fromDF(AWSGlueDataCatalog_node1712827890624DF.join(AWSGlueDataCatalog_node1712827879610DF, (AWSGlueDataCatalog_node1712827890624DF['timestamp'] == AWSGlueDataCatalog_node1712827879610DF['sensorreadingtime']), "outer"), glueContext, "Join_node1712827906770")

# Script generated for node Drop Fields
DropFields_node1712849890306 = DropFields.apply(frame=Join_node1712827906770, paths=["user"], transformation_ctx="DropFields_node1712849890306")

# Script generated for node Amazon S3
AmazonS3_node1712828071022 = glueContext.getSink(path="s3://machine-learning-curated", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1712828071022")
AmazonS3_node1712828071022.setCatalogInfo(catalogDatabase="stedi-v1",catalogTableName="machine_learning_curated_v2")
AmazonS3_node1712828071022.setFormat("json")
AmazonS3_node1712828071022.writeFrame(DropFields_node1712849890306)
job.commit()