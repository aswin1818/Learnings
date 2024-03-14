from pyspark.sql.functions import col, when
from awsglue.dynamicframe import DynamicFrame

# Function to perform data transformation
def transform_data(df):
    # Apply transformations based on your business logic

    # Example transformation: Handle SCD Type 2 changes
    # Assuming 'effective_date' and 'end_date' columns exist in the DataFrame
    df = df.withColumn("end_date", when(col("is_current") == 1, None).otherwise(df["effective_date"]))

    # Convert DataFrame to DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "transformed_data")

    return dynamic_frame

# Main function
def main():
    # Read extracted data from S3
    extracted_data_path = "s3://<BUCKET_NAME>/extracted_data"
    extracted_data = spark.read.parquet(extracted_data_path)

    # Perform transformation for each table
    transformed_data = {}
    for table_name in extracted_data.tables():
        df = extracted_data[table_name]
        transformed_data[table_name] = transform_data(df)

    # Perform additional transformations or combine data if needed

    # Write transformed data back to S3 or Redshift
    for table_name, dynamic_frame in transformed_data.items():
        transformed_data_path = f"s3://<BUCKET_NAME>/transformed_data/{table_name}"
        glueContext.write_dynamic_frame.from_options(dynamic_frame, connection_type="s3", connection_options={"path": transformed_data_path}, format="parquet")

# Entry point
if __name__ == "__main__":
    main()
