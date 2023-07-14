import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1689317062196 = glueContext.create_dynamic_frame.from_catalog(
    database="camilo-lakehouse",
    table_name="customer",
    transformation_ctx="AWSGlueDataCatalog_node1689317062196",
)

# Script generated for node remove empty values
SqlQuery0 = """
select * from myDataSource
where sharewithresearchasofdate is not null
"""
removeemptyvalues_node1689321693049 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": AWSGlueDataCatalog_node1689317062196},
    transformation_ctx="removeemptyvalues_node1689321693049",
)

# Script generated for node customer_trusted
customer_trusted_node1689317482143 = glueContext.write_dynamic_frame.from_catalog(
    frame=removeemptyvalues_node1689321693049,
    database="camilo-lakehouse",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1689317482143",
)

job.commit()
