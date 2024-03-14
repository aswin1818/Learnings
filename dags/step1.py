import sys
from datetime import datetime, timedelta
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sqlContext = SQLContext(sc)

# Get Glue job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Define function to perform incremental extraction
def incremental_extract(table_name, last_ingestion_timestamp):
    # Query OLTP database for records modified since last ingestion
    query = f"SELECT * FROM {table_name} WHERE modification_timestamp > '{last_ingestion_timestamp}'"
    df = spark.read.format("jdbc") \
        .option("url", "<JDBC_CONNECTION_URL>") \
        .option("dbtable", f"({query}) AS subquery") \
        .option("user", "<USERNAME>") \
        .option("password", "<PASSWORD>") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return df

# Main function
def main():
    # Specify tables to extract
    tables_to_extract = ["sales", "product", "customer", "store"]

    # Get last ingestion timestamp from metadata store (e.g., S3, DynamoDB)
    # Example: last_ingestion_timestamp = get_last_ingestion_timestamp()
    last_ingestion_timestamp = datetime.now() - timedelta(days=1)  # Example: 1 day ago

    # Extract data for each table
    for table_name in tables_to_extract:
        df = incremental_extract(table_name, last_ingestion_timestamp)

        # Perform any necessary transformations
        # Example: df = transform_data(df)

        # Write extracted data to S3
        s3_output_path = f"s3://<BUCKET_NAME>/extracted_data/{table_name}"
        df.write.mode("overwrite").parquet(s3_output_path)

        print(f"Extracted and saved data for table: {table_name} to {s3_output_path}")

    # Update metadata store with current ingestion timestamp
    # Example: update_metadata_store(last_ingestion_timestamp)

# Entry point
if __name__ == "__main__":
    main()
