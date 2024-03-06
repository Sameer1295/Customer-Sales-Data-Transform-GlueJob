import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1709750650986 = glueContext.create_dynamic_frame.from_catalog(
    database="clothify-catalog-db",
    table_name="customers",
    transformation_ctx="AWSGlueDataCatalog_node1709750650986",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1709750662775 = glueContext.create_dynamic_frame.from_catalog(
    database="clothify-catalog-db",
    table_name="sales",
    transformation_ctx="AWSGlueDataCatalog_node1709750662775",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1709750781210 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1709750662775,
    mappings=[
        ("order_id", "long", "right_order_id", "long"),
        ("customer_id", "long", "right_customer_id", "long"),
        ("product", "string", "right_product", "string"),
        ("quantity", "long", "right_quantity", "long"),
        ("amount", "long", "right_amount", "long"),
        ("date", "string", "right_date", "string"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1709750781210",
)

# Script generated for node Join
Join_node1709750716386 = Join.apply(
    frame1=AWSGlueDataCatalog_node1709750650986,
    frame2=RenamedkeysforJoin_node1709750781210,
    keys1=["customer_id"],
    keys2=["right_customer_id"],
    transformation_ctx="Join_node1709750716386",
)

# Script generated for node Filter
Filter_node1709750837463 = Filter.apply(
    frame=Join_node1709750716386,
    f=lambda row: (row["right_amount"] >= 800),
    transformation_ctx="Filter_node1709750837463",
)

# Script generated for node Select Fields
SelectFields_node1709750906222 = SelectFields.apply(
    frame=Filter_node1709750837463,
    paths=[
        "customer_name",
        "customer_id",
        "email",
        "right_order_id",
        "right_amount",
        "right_date",
    ],
    transformation_ctx="SelectFields_node1709750906222",
)

# Script generated for node Rename Field
RenameField_node1709750978014 = RenameField.apply(
    frame=SelectFields_node1709750906222,
    old_name="right_order_id",
    new_name="order_id",
    transformation_ctx="RenameField_node1709750978014",
)

# Script generated for node Rename Field
RenameField_node1709751010422 = RenameField.apply(
    frame=RenameField_node1709750978014,
    old_name="right_amount",
    new_name="order_amount",
    transformation_ctx="RenameField_node1709751010422",
)

# Script generated for node Rename Field
RenameField_node1709751103133 = RenameField.apply(
    frame=RenameField_node1709751010422,
    old_name="right_date",
    new_name="order_date",
    transformation_ctx="RenameField_node1709751103133",
)

# Script generated for node Amazon S3
AmazonS3_node1709751144284 = glueContext.getSink(
    path="s3://clothify-ecommerce-dataset/customersales/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1709751144284",
)
AmazonS3_node1709751144284.setCatalogInfo(
    catalogDatabase="clothify-catalog-db", catalogTableName="customer-sales"
)
AmazonS3_node1709751144284.setFormat("glueparquet", compression="snappy")
AmazonS3_node1709751144284.writeFrame(RenameField_node1709751103133)
job.commit()
