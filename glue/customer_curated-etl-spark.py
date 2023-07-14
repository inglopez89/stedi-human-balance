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

# Script generated for node accelerometer
accelerometer_node1689323425510 = glueContext.create_dynamic_frame.from_catalog(
    database="camilo-lakehouse",
    table_name="accelerometer",
    transformation_ctx="accelerometer_node1689323425510",
)

# Script generated for node customer_trusted
customer_trusted_node1689323411685 = glueContext.create_dynamic_frame.from_catalog(
    database="camilo-lakehouse",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1689323411685",
)

# Script generated for node Join
Join_node1689323436362 = Join.apply(
    frame1=customer_trusted_node1689323411685,
    frame2=accelerometer_node1689323425510,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1689323436362",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1689324039218 = glueContext.write_dynamic_frame.from_catalog(
    frame=Join_node1689323436362,
    database="camilo-lakehouse",
    table_name="customer_curated",
    transformation_ctx="AWSGlueDataCatalog_node1689324039218",
)

job.commit()
