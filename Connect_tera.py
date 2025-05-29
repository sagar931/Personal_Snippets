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




_______

import pandas as pd
from sqlalchemy import create_engine

# Connection config
username = 'your_username'
password = 'your_password'
host = 'your_teradata_host'  # e.g., tdprod.company.com

# SQLAlchemy connection string
engine = create_engine(f"teradatasql://{username}:{password}@{host}")

# Read CSV
df = pd.read_csv("your_file.csv")

# Append to existing table in Teradata
try:
    df.to_sql(
        name='your_table_name',     # Target table
        con=engine,
        schema='your_schema',       # Optional: if you have a schema
        if_exists='append',         # Append to the existing table
        index=False,                # Do not write DataFrame index
        method='multi'              # Batch insert
    )
    print("Data appended successfully.")

except Exception as e:
    print("Error while appending data:", e)
