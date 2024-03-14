import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Get Glue job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Main function to copy data to Redshift
def main():
    # Specify Redshift connection details
    jdbc_url = "<REDSHIFT_JDBC_URL>"
    redshift_table = "<REDSHIFT_TABLE>"
    redshift_user = "<REDSHIFT_USERNAME>"
    redshift_password = "<REDSHIFT_PASSWORD>"

    # Specify S3 path where data is stored
    s3_input_path = "s3://<BUCKET_NAME>/extracted_data/"

    # Read data from S3 into DataFrame
    df = spark.read.parquet(s3_input_path)

    # Write DataFrame to Redshift
    df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", redshift_table) \
        .option("user", redshift_user) \
        .option("password", redshift_password) \
        .option("driver", "com.amazon.redshift.jdbc42.Driver") \
        .mode("append") \
        .save()

# Entry point
if __name__ == "__main__":
    main()
