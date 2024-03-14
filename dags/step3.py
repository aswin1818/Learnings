import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SQLContext

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sqlContext = SQLContext(sc)

# Get Glue job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Main function to load data into data lake (S3)
def main():
    # Specify tables to load
    tables_to_load = ["sales", "product", "customer", "store"]

    # Iterate over tables and load data into S3
    for table_name in tables_to_load:
        # Read data from Parquet files in S3
        s3_input_path = f"s3://<BUCKET_NAME>/extracted_data/{table_name}"
        df = spark.read.parquet(s3_input_path)

        # Write data to a new location in S3
        s3_output_path = f"s3://<BUCKET_NAME>/data_lake/{table_name}/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
        df.write.mode("overwrite").parquet(s3_output_path)

        print(f"Loaded data for table: {table_name} to {s3_output_path}")

# Entry point
if __name__ == "__main__":
    main()
