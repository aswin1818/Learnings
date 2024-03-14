import boto3

# Initialize Redshift client
redshift = boto3.client('redshift')

# Define function to vacuum and analyze tables
def optimize_tables(cluster_id, db_name, table_names):
    for table_name in table_names:
        # Vacuum the table to reclaim space and update statistics
        response_vacuum = redshift.modify_cluster_db(
            ClusterIdentifier=cluster_id,
            DatabaseName=db_name,
            DbName=db_name,
            WithOptions=f"VACUUM {table_name}"
        )
        print(f"Vacuumed table {table_name}: {response_vacuum}")

        # Analyze the table to update query planner statistics
        response_analyze = redshift.modify_cluster_db(
            ClusterIdentifier=cluster_id,
            DatabaseName=db_name,
            DbName=db_name,
            WithOptions=f"ANALYZE {table_name}"
        )
        print(f"Analyzed table {table_name}: {response_analyze}")

# Main function
def main():
    # Specify the Redshift cluster ID
    cluster_id = "<YOUR_REDSHIFT_CLUSTER_ID>"

    # Specify the database name
    db_name = "<YOUR_DATABASE_NAME>"

    # Specify the tables to optimize
    table_names = ["table1", "table2", "table3"]  # Add more tables as needed

    # Perform vacuum and analyze for each table
    optimize_tables(cluster_id, db_name, table_names)

# Entry point
if __name__ == "__main__":
    main()
