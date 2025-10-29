"""
PySpark Manual Testing Commands
Copy-paste these commands one by one in your Jupyter notebook for manual testing
"""

# ============================================================================
# PREREQUISITE: Ensure you have an active SparkSession
# ============================================================================

# Check if spark session exists
print(spark)
print(f"Spark Version: {spark.version}")
print(f"User: {spark.sparkContext.sparkUser()}")


# ============================================================================
# 1. DATABASE OPERATIONS
# ============================================================================

# --- Show all databases ---
spark.sql("SHOW DATABASES").show()

# --- Use a specific database ---
spark.sql("USE your_database_name")

# --- Show current database ---
spark.sql("SELECT current_database()").show()

# --- Show all tables in current database ---
spark.sql("SHOW TABLES").show()

# --- Show tables in specific database ---
spark.sql("SHOW TABLES IN your_database_name").show()


# ============================================================================
# 2. CREATE TABLE
# ============================================================================

# --- Method 1: Create table using SQL ---
spark.sql("""
    CREATE TABLE IF NOT EXISTS test_database.test_table (
        id INT,
        name STRING,
        age INT,
        salary DOUBLE,
        created_date DATE
    )
    STORED AS PARQUET
""")

# --- Method 2: Create table from DataFrame ---
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from datetime import date

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True)
])

# Create empty DataFrame
df = spark.createDataFrame([], schema)

# Save as table
df.write.mode("overwrite").saveAsTable("test_database.test_table2")

# --- Method 3: Create partitioned table ---
spark.sql("""
    CREATE TABLE IF NOT EXISTS test_database.test_table_partitioned (
        id INT,
        name STRING,
        value DOUBLE
    )
    PARTITIONED BY (year INT, month INT)
    STORED AS PARQUET
""")

# --- Method 4: Create external table ---
spark.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS test_database.test_external (
        id INT,
        data STRING
    )
    STORED AS PARQUET
    LOCATION '/tmp/test_external_location'
""")

# --- Verify table was created ---
spark.sql("SHOW TABLES IN test_database").show()
spark.sql("DESCRIBE FORMATTED test_database.test_table").show(100, truncate=False)


# ============================================================================
# 3. INSERT / WRITE DATA
# ============================================================================

# --- Method 1: Insert using SQL VALUES ---
spark.sql("""
    INSERT INTO test_database.test_table 
    VALUES 
        (1, 'Alice', 30, 50000.0, '2024-01-01'),
        (2, 'Bob', 25, 45000.0, '2024-01-02'),
        (3, 'Charlie', 35, 60000.0, '2024-01-03')
""")

# --- Method 2: Insert from another table ---
spark.sql("""
    INSERT INTO test_database.test_table
    SELECT * FROM test_database.source_table
    WHERE some_condition = 'value'
""")

# --- Method 3: Insert using DataFrame (Append mode) ---
from datetime import date

data = [
    (4, 'David', 28, 52000.0, date(2024, 1, 4)),
    (5, 'Eve', 32, 58000.0, date(2024, 1, 5))
]

columns = ['id', 'name', 'age', 'salary', 'created_date']
df = spark.createDataFrame(data, columns)

# Append to table
df.write.mode('append').insertInto('test_database.test_table')

# --- Method 4: Insert overwrite (replace all data) ---
df.write.mode('overwrite').insertInto('test_database.test_table')

# --- Method 5: Insert into partitioned table ---
spark.sql("""
    INSERT INTO test_database.test_table_partitioned 
    PARTITION(year=2024, month=10)
    VALUES (1, 'test_data', 100.5)
""")

# Using DataFrame for partitioned table
df_part = spark.createDataFrame([(1, 'data1', 100.0, 2024, 10)], 
                                 ['id', 'name', 'value', 'year', 'month'])
df_part.write.mode('append').insertInto('test_database.test_table_partitioned')

# --- Method 6: Save DataFrame as new table ---
df.write.mode('overwrite').saveAsTable('test_database.new_table')

# --- Method 7: Write to HDFS location directly ---
df.write.mode('overwrite').parquet('/user/your_username/test_data')

# --- Verify data was inserted ---
spark.sql("SELECT COUNT(*) as count FROM test_database.test_table").show()


# ============================================================================
# 4. READ DATA
# ============================================================================

# --- Method 1: Read table using spark.table() ---
df = spark.table("test_database.test_table")
df.show()

# --- Method 2: Read using SQL ---
df = spark.sql("SELECT * FROM test_database.test_table")
df.show()

# --- Method 3: Read with filter ---
df = spark.sql("SELECT * FROM test_database.test_table WHERE age > 30")
df.show()

# --- Method 4: Read specific columns ---
df = spark.sql("SELECT id, name, salary FROM test_database.test_table")
df.show()

# --- Method 5: Read with aggregation ---
df = spark.sql("""
    SELECT 
        COUNT(*) as total_records,
        AVG(age) as avg_age,
        SUM(salary) as total_salary,
        MAX(salary) as max_salary,
        MIN(salary) as min_salary
    FROM test_database.test_table
""")
df.show()

# --- Method 6: Read using DataFrame API ---
df = spark.table("test_database.test_table")
df.select("id", "name", "age").show()
df.filter(df.age > 30).show()
df.groupBy().agg({"salary": "avg", "age": "max"}).show()

# --- Method 7: Read from HDFS path ---
df = spark.read.parquet("/user/your_username/test_data")
df.show()

# --- Method 8: Read partitioned data ---
df = spark.table("test_database.test_table_partitioned")
df.show()

# Filter by partition
df_filtered = spark.sql("""
    SELECT * FROM test_database.test_table_partitioned
    WHERE year = 2024 AND month = 10
""")
df_filtered.show()

# --- View table schema ---
df.printSchema()

# --- Show sample data ---
df.show(10)  # Show 10 rows
df.show(10, truncate=False)  # Show full content without truncation

# --- Get row count ---
row_count = df.count()
print(f"Total rows: {row_count}")

# --- Convert to Pandas (for small datasets only!) ---
pandas_df = df.limit(100).toPandas()
print(pandas_df.head())


# ============================================================================
# 5. UPDATE DATA (Spark doesn't support UPDATE directly, use overwrite)
# ============================================================================

# --- Method 1: Update using overwrite with transformation ---
# Read existing data
df = spark.table("test_database.test_table")

# Apply transformation (e.g., update salary)
from pyspark.sql.functions import when, col

df_updated = df.withColumn(
    "salary",
    when(col("age") > 30, col("salary") * 1.1)  # 10% raise for age > 30
    .otherwise(col("salary"))
)

# Overwrite table
df_updated.write.mode("overwrite").saveAsTable("test_database.test_table")

# --- Method 2: Update using SQL (Hive/Delta Lake syntax if supported) ---
# Note: This requires Delta Lake or Hive ACID tables
spark.sql("""
    UPDATE test_database.test_table
    SET salary = salary * 1.1
    WHERE age > 30
""")

# --- Method 3: Add new column ---
spark.sql("""
    ALTER TABLE test_database.test_table 
    ADD COLUMNS (department STRING)
""")

# --- Method 4: Rename column ---
spark.sql("""
    ALTER TABLE test_database.test_table 
    CHANGE COLUMN name employee_name STRING
""")

# --- Method 5: Change column data type ---
spark.sql("""
    ALTER TABLE test_database.test_table 
    CHANGE COLUMN age age BIGINT
""")


# ============================================================================
# 6. DELETE DATA
# ============================================================================

# --- Method 1: Delete specific rows (using overwrite with filter) ---
df = spark.table("test_database.test_table")
df_filtered = df.filter(df.age >= 25)  # Keep only rows where age >= 25
df_filtered.write.mode("overwrite").saveAsTable("test_database.test_table")

# --- Method 2: Delete using SQL (if Delta Lake or ACID tables) ---
spark.sql("""
    DELETE FROM test_database.test_table
    WHERE age < 25
""")

# --- Method 3: Truncate table (delete all data, keep structure) ---
spark.sql("TRUNCATE TABLE test_database.test_table")

# --- Method 4: Delete partition ---
spark.sql("""
    ALTER TABLE test_database.test_table_partitioned
    DROP IF EXISTS PARTITION(year=2024, month=10)
""")


# ============================================================================
# 7. DROP / DELETE TABLE
# ============================================================================

# --- Drop table (removes table and data) ---
spark.sql("DROP TABLE IF EXISTS test_database.test_table")

# --- Drop external table (keeps data in HDFS) ---
spark.sql("DROP TABLE IF EXISTS test_database.test_external")

# --- Drop database (must be empty) ---
spark.sql("DROP DATABASE IF EXISTS test_database")

# --- Drop database with all tables (CASCADE) ---
spark.sql("DROP DATABASE IF EXISTS test_database CASCADE")

# --- Verify table is dropped ---
spark.sql("SHOW TABLES IN test_database").show()


# ============================================================================
# 8. TABLE METADATA & INFORMATION
# ============================================================================

# --- Show table schema ---
spark.sql("DESCRIBE test_database.test_table").show()

# --- Show detailed table information ---
spark.sql("DESCRIBE FORMATTED test_database.test_table").show(100, truncate=False)

# --- Show extended table info ---
spark.sql("DESCRIBE EXTENDED test_database.test_table").show(100, truncate=False)

# --- Show table properties ---
spark.sql("SHOW TBLPROPERTIES test_database.test_table").show()

# --- Show partitions ---
spark.sql("SHOW PARTITIONS test_database.test_table_partitioned").show()

# --- Show table location ---
spark.sql("DESCRIBE FORMATTED test_database.test_table").filter("col_name = 'Location'").show(truncate=False)

# --- Get table statistics ---
spark.sql("ANALYZE TABLE test_database.test_table COMPUTE STATISTICS")
spark.sql("DESCRIBE EXTENDED test_database.test_table").filter("col_name = 'Statistics'").show(truncate=False)


# ============================================================================
# 9. PRACTICAL TESTING SEQUENCE
# ============================================================================

"""
COMPLETE TESTING SEQUENCE - Copy this entire block
"""

print("="*80)
print("STARTING MANUAL PERMISSION TEST")
print("="*80)

# STEP 1: Set your test database
test_db = "your_test_database"  # CHANGE THIS!
test_table = f"{test_db}.manual_test_table"

print(f"\n1. Testing database: {test_db}")
spark.sql(f"USE {test_db}")
print("   ✓ Database accessed")

# STEP 2: Create table
print(f"\n2. Creating table: {test_table}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {test_table} (
        id INT,
        name STRING,
        value DOUBLE,
        created_at TIMESTAMP
    )
    STORED AS PARQUET
""")
print("   ✓ Table created")

# STEP 3: Insert data
print(f"\n3. Inserting data into: {test_table}")
spark.sql(f"""
    INSERT INTO {test_table}
    VALUES 
        (1, 'Test1', 100.5, current_timestamp()),
        (2, 'Test2', 200.75, current_timestamp()),
        (3, 'Test3', 300.25, current_timestamp())
""")
print("   ✓ Data inserted")

# STEP 4: Read data
print(f"\n4. Reading data from: {test_table}")
df = spark.table(test_table)
print(f"   Row count: {df.count()}")
df.show()
print("   ✓ Data read successfully")

# STEP 5: Update data (via overwrite)
print(f"\n5. Updating data in: {test_table}")
from pyspark.sql.functions import col
df_updated = df.withColumn("value", col("value") * 2)
df_updated.write.mode("overwrite").saveAsTable(test_table)
print("   ✓ Data updated")

# Verify update
df_check = spark.table(test_table)
df_check.select("id", "value").show()

# STEP 6: Delete data (truncate)
print(f"\n6. Truncating table: {test_table}")
spark.sql(f"TRUNCATE TABLE {test_table}")
count_after = spark.table(test_table).count()
print(f"   Row count after truncate: {count_after}")
print("   ✓ Data deleted")

# STEP 7: Drop table
print(f"\n7. Dropping table: {test_table}")
spark.sql(f"DROP TABLE IF EXISTS {test_table}")
print("   ✓ Table dropped")

# STEP 8: Verify table is gone
print(f"\n8. Verifying table deletion")
tables = spark.sql(f"SHOW TABLES IN {test_db}").collect()
table_names = [row.tableName for row in tables]
if "manual_test_table" not in table_names:
    print("   ✓ Table successfully removed")
else:
    print("   ✗ Table still exists!")

print("\n" + "="*80)
print("MANUAL PERMISSION TEST COMPLETED")
print("="*80)


# ============================================================================
# 10. QUICK COMMAND REFERENCE
# ============================================================================

"""
QUICK COMMAND CHEATSHEET:

DATABASE:
  spark.sql("SHOW DATABASES").show()
  spark.sql("USE database_name")
  spark.sql("SHOW TABLES").show()

CREATE:
  spark.sql("CREATE TABLE db.table (col1 INT, col2 STRING)")
  df.write.saveAsTable("db.table")

INSERT:
  spark.sql("INSERT INTO db.table VALUES (1, 'data')")
  df.write.mode('append').insertInto('db.table')

READ:
  df = spark.table("db.table")
  df = spark.sql("SELECT * FROM db.table")
  df.show()

UPDATE:
  df_updated = df.withColumn("col", col("col") * 2)
  df_updated.write.mode("overwrite").saveAsTable("db.table")

DELETE:
  spark.sql("TRUNCATE TABLE db.table")
  df_filtered.write.mode("overwrite").saveAsTable("db.table")

DROP:
  spark.sql("DROP TABLE db.table")

METADATA:
  spark.sql("DESCRIBE db.table").show()
  spark.sql("DESCRIBE FORMATTED db.table").show()
"""
