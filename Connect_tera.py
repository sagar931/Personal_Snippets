import teradatasql

# Connection credentials
host = 'your_teradata_host'     # e.g., 'tdprod.company.com'
username = 'your_username'
password = 'your_password'

# Connect to Teradata
try:
    with teradatasql.connect(
        host=host,
        user=username,
        password=password
    ) as conn:

        # Create a cursor
        with conn.cursor() as cursor:
            # Run a simple query
            cursor.execute("SELECT TOP 10 * FROM your_database.your_table")

            # Fetch and print results
            rows = cursor.fetchall()
            for row in rows:
                print(row)

except Exception as e:
    print("Connection or query failed:", e)
