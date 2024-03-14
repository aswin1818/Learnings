import psycopg2

def refresh_materialized_views():
    try:
        # Connect to Redshift cluster
        conn = psycopg2.connect(
            dbname='<YOUR_DB_NAME>',
            user='<YOUR_DB_USER>',
            password='<YOUR_DB_PASSWORD>',
            host='<YOUR_REDSHIFT_ENDPOINT>',
            port='<YOUR_REDSHIFT_PORT>'
        )
        cursor = conn.cursor()

        # List of materialized views to refresh
        materialized_views = ['mv_sales_summary', 'mv_product_summary', 'mv_customer_summary']

        # Refresh each materialized view
        for view_name in materialized_views:
            # Refresh materialized view
            refresh_query = f'REFRESH MATERIALIZED VIEW {view_name};'
            cursor.execute(refresh_query)
            conn.commit()
            print(f'{view_name} refreshed successfully.')

        # Close connection
        cursor.close()
        conn.close()
        print('All materialized views refreshed successfully.')

    except Exception as e:
        print(f'Error occurred: {str(e)}')

# Execute refresh function
refresh_materialized_views()
