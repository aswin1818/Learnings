import psycopg2
import boto3
import pandas as pd
from datetime import datetime

# Connect to PostgreSQL database
conn_pg = psycopg2.connect(
    dbname='your_postgres_db',
    user='your_username',
    password='your_password',
    host='your_host',
    port='your_port'
)

# Open a cursor to perform database operations
cursor_pg = conn_pg.cursor()

# Extract data from PostgreSQL for SCD handling
cursor_pg.execute('SELECT * FROM product')
products_data = cursor_pg.fetchall()

# Close PostgreSQL cursor and connection
cursor_pg.close()
conn_pg.close()

# Convert PostgreSQL data into DataFrame for easier manipulation
products_df = pd.DataFrame(products_data, columns=['product_id', 'product_name', 'category', 'price'])

# Check if there are any changes in product prices
# For demonstration purposes, let's assume we have a new product price data
new_price_data = {
    'product_id': [1, 2, 3],  # Example product IDs with updated prices
    'new_price': [15.00, 25.00, 12.50]  # Example new prices
}
new_prices_df = pd.DataFrame(new_price_data)

# Identify products with updated prices
updated_products_df = pd.merge(products_df, new_prices_df, on='product_id', how='inner')

# Connect to S3 for staging updated product data
s3 = boto3.client('s3')

# Write updated product data to S3
updated_products_df.to_csv('updated_product_prices.csv', index=False)
s3.upload_file('updated_product_prices.csv', 'your_bucket_name', 'staging/updated_product_prices.csv')

# Connect to Redshift database
conn_rs = psycopg2.connect(
    dbname='your_redshift_db',
    user='your_username',
    password='your_password',
    host='your_host',
    port='your_port'
)

# Open a cursor to perform database operations
cursor_rs = conn_rs.cursor()

# Perform SCD Type 2 handling: Update existing records with end date and insert new records for updated products
for index, row in updated_products_df.iterrows():
    product_id = row['product_id']
    new_price = row['new_price']

    # Update existing record with end date
    update_query = f"""
    UPDATE product
    SET end_date = '{datetime.now()}'
    WHERE product_id = {product_id} AND end_date IS NULL;
    """
    cursor_rs.execute(update_query)

    # Insert new record for updated product
    insert_query = f"""
    INSERT INTO product (product_id, product_name, category, price, start_date)
    VALUES ({product_id}, '{row['product_name']}', '{row['category']}', {new_price}, '{datetime.now()}');
    """
    cursor_rs.execute(insert_query)

# Commit changes and close cursor and connection
conn_rs.commit()
cursor_rs.close()
conn_rs.close()
