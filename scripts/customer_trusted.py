import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1712900185738 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://customer-landing-v1"], "recurse": True}, transformation_ctx="AmazonS3_node1712900185738")

# Script generated for node Filter
Filter_node1712900208221 = Filter.apply(frame=AmazonS3_node1712900185738, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="Filter_node1712900208221")

# Script generated for node Amazon S3
AmazonS3_node1712900228698 = glueContext.getSink(path="s3://customer-trusted-v1", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1712900228698")
AmazonS3_node1712900228698.setCatalogInfo(catalogDatabase="stedi-v1",catalogTableName="customer_trusted_v2")
AmazonS3_node1712900228698.setFormat("json")
AmazonS3_node1712900228698.writeFrame(Filter_node1712900208221)
job.commit()