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

# Script generated for node customer_curated
customer_curated_node1689325519267 = glueContext.create_dynamic_frame.from_catalog(
    database="camilo-lakehouse",
    table_name="customer_curated",
    transformation_ctx="customer_curated_node1689325519267",
)

# Script generated for node step_training_landing
step_training_landing_node1689325544165 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://camilo-data-lake/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_training_landing_node1689325544165",
)

# Script generated for node Join
Join_node1689325592999 = Join.apply(
    frame1=step_training_landing_node1689325544165,
    frame2=customer_curated_node1689325519267,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1689325592999",
)

# Script generated for node drop and change to lowercase name fields
dropandchangetolowercasenamefields_node1689326098216 = ApplyMapping.apply(
    frame=Join_node1689325592999,
    mappings=[
        ("customername", "string", "customername", "string"),
        ("email", "string", "email", "string"),
        ("phone", "string", "phone", "string"),
        ("birthday", "string", "birthday", "string"),
        ("serialnumber", "string", "serialnumber", "string"),
        ("registrationdate", "long", "registrationdate", "long"),
        ("lastupdatedate", "long", "lastupdatedate", "long"),
        ("sharewithresearchasofdate", "long", "sharewithresearchasofdate", "long"),
        ("sharewithpublicasofdate", "long", "sharewithpublicasofdate", "long"),
        ("sharewithfriendsasofdate", "long", "sharewithfriendsasofdate", "long"),
        ("user", "string", "user", "string"),
        ("timestamp", "timestamp", "timestamp", "timestamp"),
        ("x", "double", "x", "double"),
        ("y", "double", "y", "double"),
        ("z", "double", "z", "double"),
        ("sensorReadingTime", "bigint", "sensorreadingtime", "long"),
        ("distanceFromObject", "int", "distancefromobject", "long"),
    ],
    transformation_ctx="dropandchangetolowercasenamefields_node1689326098216",
)

# Script generated for node Step_trainer_trusted
Step_trainer_trusted_node1689325767442 = glueContext.write_dynamic_frame.from_catalog(
    frame=dropandchangetolowercasenamefields_node1689326098216,
    database="camilo-lakehouse",
    table_name="step_trainer_trusted",
    transformation_ctx="Step_trainer_trusted_node1689325767442",
)

job.commit()
