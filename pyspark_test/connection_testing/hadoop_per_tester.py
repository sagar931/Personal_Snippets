"""
Hadoop Permissions Testing Suite for Jupyter Notebook
Complete test suite to verify CREATE, READ, UPDATE, DELETE permissions
Includes tracking table and manager-ready checklist
"""

import pandas as pd
from datetime import datetime
from IPython.display import display, HTML
import traceback

class HadoopPermissionTester:
    """
    Comprehensive testing suite for Hadoop permissions via PySpark
    Tests CREATE, READ, UPDATE, DELETE operations on specified databases/tables
    """
    
    def __init__(self, spark, test_database, test_table_prefix="permission_test"):
        """
        Initialize the tester
        
        Args:
            spark: Active SparkSession
            test_database: Database to use for testing (MUST BE NON-PRODUCTION!)
            test_table_prefix: Prefix for test tables (default: permission_test)
        """
        self.spark = spark
        self.test_database = test_database
        self.test_table_prefix = test_table_prefix
        self.test_results = []
        self.current_user = spark.sparkContext.sparkUser()
        self.test_start_time = datetime.now()
        
        print("="*80)
        print("HADOOP PERMISSIONS TESTING SUITE")
        print("="*80)
        print(f"User: {self.current_user}")
        print(f"Test Database: {test_database}")
        print(f"Test Table Prefix: {test_table_prefix}")
        print(f"Start Time: {self.test_start_time}")
        print("="*80)
        print("\n⚠️  WARNING: This will create, modify, and delete test data!")
        print("⚠️  Ensure you're using a NON-PRODUCTION database!\n")
    
    def _add_result(self, test_name, category, operation, status, message, details=""):
        """Add test result to tracking list"""
        result = {
            'Test #': len(self.test_results) + 1,
            'Test Name': test_name,
            'Category': category,
            'Operation': operation,
            'Status': status,
            'Message': message,
            'Details': details,
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        self.test_results.append(result)
        
        # Print immediate feedback
        status_symbol = "✓" if status == "PASS" else "✗" if status == "FAIL" else "⚠"
        print(f"{status_symbol} Test #{result['Test #']}: {test_name} - {status}")
        if status == "FAIL":
            print(f"   Error: {message}")
    
    # ========== TEST 1: DATABASE ACCESS ==========
    def test_database_access(self):
        """Test 1: Verify database access"""
        print("\n" + "="*80)
        print("TEST CATEGORY 1: DATABASE ACCESS")
        print("="*80 + "\n")
        
        # Test 1.1: List all databases
        try:
            databases = self.spark.sql("SHOW DATABASES").collect()
            db_list = [row.databaseName for row in databases]
            
            if self.test_database in db_list:
                self._add_result(
                    "List Databases & Verify Test DB Exists",
                    "Database Access",
                    "READ",
                    "PASS",
                    f"Can list databases. Test database '{self.test_database}' exists.",
                    f"Total databases visible: {len(db_list)}"
                )
            else:
                self._add_result(
                    "List Databases & Verify Test DB Exists",
                    "Database Access",
                    "READ",
                    "FAIL",
                    f"Test database '{self.test_database}' does not exist!",
                    f"Available databases: {', '.join(db_list[:10])}"
                )
                return False
        except Exception as e:
            self._add_result(
                "List Databases & Verify Test DB Exists",
                "Database Access",
                "READ",
                "FAIL",
                f"Cannot list databases: {str(e)}",
                traceback.format_exc()
            )
            return False
        
        # Test 1.2: Use the database
        try:
            self.spark.sql(f"USE {self.test_database}")
            self._add_result(
                "Switch to Test Database",
                "Database Access",
                "READ",
                "PASS",
                f"Successfully switched to database '{self.test_database}'"
            )
        except Exception as e:
            self._add_result(
                "Switch to Test Database",
                "Database Access",
                "READ",
                "FAIL",
                f"Cannot use database: {str(e)}",
                traceback.format_exc()
            )
            return False
        
        # Test 1.3: List tables in database
        try:
            tables = self.spark.sql(f"SHOW TABLES IN {self.test_database}").collect()
            table_count = len(tables)
            self._add_result(
                "List Tables in Database",
                "Database Access",
                "READ",
                "PASS",
                f"Can list tables in '{self.test_database}'",
                f"Total tables: {table_count}"
            )
        except Exception as e:
            self._add_result(
                "List Tables in Database",
                "Database Access",
                "READ",
                "FAIL",
                f"Cannot list tables: {str(e)}",
                traceback.format_exc()
            )
            return False
        
        return True
    
    # ========== TEST 2: CREATE PERMISSIONS ==========
    def test_create_permissions(self):
        """Test 2: Verify CREATE permissions"""
        print("\n" + "="*80)
        print("TEST CATEGORY 2: CREATE PERMISSIONS")
        print("="*80 + "\n")
        
        test_table = f"{self.test_database}.{self.test_table_prefix}_create_test"
        
        # Test 2.1: Create a simple table
        try:
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {test_table} (
                id INT,
                name STRING,
                value DOUBLE,
                created_at TIMESTAMP
            )
            STORED AS PARQUET
            """
            self.spark.sql(create_sql)
            
            # Verify table was created
            tables = self.spark.sql(f"SHOW TABLES IN {self.test_database}").collect()
            table_names = [row.tableName for row in tables]
            
            if f"{self.test_table_prefix}_create_test" in table_names:
                self._add_result(
                    "Create New Table",
                    "Create Permissions",
                    "CREATE",
                    "PASS",
                    f"Successfully created table '{test_table}'"
                )
            else:
                self._add_result(
                    "Create New Table",
                    "Create Permissions",
                    "CREATE",
                    "FAIL",
                    "Table creation command executed but table not found"
                )
                return False
        except Exception as e:
            self._add_result(
                "Create New Table",
                "Create Permissions",
                "CREATE",
                "FAIL",
                f"Cannot create table: {str(e)}",
                traceback.format_exc()
            )
            return False
        
        # Test 2.2: Create table with partitions
        try:
            partitioned_table = f"{self.test_database}.{self.test_table_prefix}_partitioned"
            create_part_sql = f"""
            CREATE TABLE IF NOT EXISTS {partitioned_table} (
                id INT,
                data STRING
            )
            PARTITIONED BY (year INT, month INT)
            STORED AS PARQUET
            """
            self.spark.sql(create_part_sql)
            self._add_result(
                "Create Partitioned Table",
                "Create Permissions",
                "CREATE",
                "PASS",
                f"Successfully created partitioned table '{partitioned_table}'"
            )
        except Exception as e:
            self._add_result(
                "Create Partitioned Table",
                "Create Permissions",
                "CREATE",
                "FAIL",
                f"Cannot create partitioned table: {str(e)}",
                traceback.format_exc()
            )
        
        # Test 2.3: Create external table
        try:
            external_table = f"{self.test_database}.{self.test_table_prefix}_external"
            external_location = f"/tmp/{self.current_user}/test_external_table"
            
            create_ext_sql = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {external_table} (
                id INT,
                name STRING
            )
            STORED AS PARQUET
            LOCATION '{external_location}'
            """
            self.spark.sql(create_ext_sql)
            self._add_result(
                "Create External Table",
                "Create Permissions",
                "CREATE",
                "PASS",
                f"Successfully created external table at '{external_location}'"
            )
        except Exception as e:
            self._add_result(
                "Create External Table",
                "Create Permissions",
                "CREATE",
                "FAIL",
                f"Cannot create external table: {str(e)}",
                traceback.format_exc()
            )
        
        return True
    
    # ========== TEST 3: INSERT/WRITE PERMISSIONS ==========
    def test_insert_permissions(self):
        """Test 3: Verify INSERT/WRITE permissions"""
        print("\n" + "="*80)
        print("TEST CATEGORY 3: INSERT/WRITE PERMISSIONS")
        print("="*80 + "\n")
        
        test_table = f"{self.test_database}.{self.test_table_prefix}_create_test"
        
        # Test 3.1: Insert data using SQL
        try:
            insert_sql = f"""
            INSERT INTO {test_table} VALUES
                (1, 'Alice', 100.5, current_timestamp()),
                (2, 'Bob', 200.75, current_timestamp()),
                (3, 'Charlie', 300.25, current_timestamp())
            """
            self.spark.sql(insert_sql)
            
            # Verify data was inserted
            count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {test_table}").collect()[0].cnt
            
            if count >= 3:
                self._add_result(
                    "Insert Data via SQL",
                    "Write Permissions",
                    "INSERT",
                    "PASS",
                    f"Successfully inserted {count} rows"
                )
            else:
                self._add_result(
                    "Insert Data via SQL",
                    "Write Permissions",
                    "INSERT",
                    "FAIL",
                    f"Expected 3+ rows, found {count}"
                )
        except Exception as e:
            self._add_result(
                "Insert Data via SQL",
                "Write Permissions",
                "INSERT",
                "FAIL",
                f"Cannot insert data: {str(e)}",
                traceback.format_exc()
            )
            return False
        
        # Test 3.2: Insert data using DataFrame
        try:
            data = [(4, 'David', 400.0, datetime.now()),
                    (5, 'Eve', 500.5, datetime.now())]
            columns = ['id', 'name', 'value', 'created_at']
            df = self.spark.createDataFrame(data, columns)
            
            df.write.mode('append').insertInto(test_table)
            
            # Verify
            count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {test_table}").collect()[0].cnt
            self._add_result(
                "Insert Data via DataFrame",
                "Write Permissions",
                "INSERT",
                "PASS",
                f"Successfully inserted via DataFrame. Total rows: {count}"
            )
        except Exception as e:
            self._add_result(
                "Insert Data via DataFrame",
                "Write Permissions",
                "INSERT",
                "FAIL",
                f"Cannot insert via DataFrame: {str(e)}",
                traceback.format_exc()
            )
        
        # Test 3.3: Insert with partition
        try:
            partitioned_table = f"{self.test_database}.{self.test_table_prefix}_partitioned"
            insert_part_sql = f"""
            INSERT INTO {partitioned_table} PARTITION(year=2025, month=10)
            VALUES (1, 'test_data_1'), (2, 'test_data_2')
            """
            self.spark.sql(insert_part_sql)
            self._add_result(
                "Insert Data into Partitioned Table",
                "Write Permissions",
                "INSERT",
                "PASS",
                "Successfully inserted into partitioned table"
            )
        except Exception as e:
            self._add_result(
                "Insert Data into Partitioned Table",
                "Write Permissions",
                "INSERT",
                "FAIL",
                f"Cannot insert into partitioned table: {str(e)}",
                traceback.format_exc()
            )
        
        return True
    
    # ========== TEST 4: READ PERMISSIONS ==========
    def test_read_permissions(self):
        """Test 4: Verify READ permissions"""
        print("\n" + "="*80)
        print("TEST CATEGORY 4: READ PERMISSIONS")
        print("="*80 + "\n")
        
        test_table = f"{self.test_database}.{self.test_table_prefix}_create_test"
        
        # Test 4.1: SELECT query
        try:
            result = self.spark.sql(f"SELECT * FROM {test_table}").collect()
            row_count = len(result)
            self._add_result(
                "SELECT Query (Read All Data)",
                "Read Permissions",
                "SELECT",
                "PASS",
                f"Successfully read {row_count} rows from table"
            )
        except Exception as e:
            self._add_result(
                "SELECT Query (Read All Data)",
                "Read Permissions",
                "SELECT",
                "FAIL",
                f"Cannot read data: {str(e)}",
                traceback.format_exc()
            )
            return False
        
        # Test 4.2: Aggregation query
        try:
            result = self.spark.sql(f"""
                SELECT COUNT(*) as cnt, SUM(value) as total, AVG(value) as avg_val
                FROM {test_table}
            """).collect()
            
            stats = result[0]
            self._add_result(
                "Aggregation Query",
                "Read Permissions",
                "SELECT",
                "PASS",
                f"Aggregation successful: {stats.cnt} rows, total={stats.total:.2f}"
            )
        except Exception as e:
            self._add_result(
                "Aggregation Query",
                "Read Permissions",
                "SELECT",
                "FAIL",
                f"Cannot perform aggregation: {str(e)}",
                traceback.format_exc()
            )
        
        # Test 4.3: Read as DataFrame
        try:
            df = self.spark.table(test_table)
            schema = df.schema
            count = df.count()
            
            self._add_result(
                "Read via DataFrame API",
                "Read Permissions",
                "SELECT",
                "PASS",
                f"DataFrame created successfully. Schema: {len(schema.fields)} columns, {count} rows"
            )
        except Exception as e:
            self._add_result(
                "Read via DataFrame API",
                "Read Permissions",
                "SELECT",
                "FAIL",
                f"Cannot read via DataFrame: {str(e)}",
                traceback.format_exc()
            )
        
        # Test 4.4: Describe table
        try:
            desc = self.spark.sql(f"DESCRIBE FORMATTED {test_table}").collect()
            self._add_result(
                "Describe Table (Metadata Access)",
                "Read Permissions",
                "SELECT",
                "PASS",
                f"Can access table metadata ({len(desc)} properties)"
            )
        except Exception as e:
            self._add_result(
                "Describe Table (Metadata Access)",
                "Read Permissions",
                "SELECT",
                "FAIL",
                f"Cannot describe table: {str(e)}",
                traceback.format_exc()
            )
        
        return True
    
    # ========== TEST 5: UPDATE PERMISSIONS ==========
    def test_update_permissions(self):
        """Test 5: Verify UPDATE permissions"""
        print("\n" + "="*80)
        print("TEST CATEGORY 5: UPDATE PERMISSIONS")
        print("="*80 + "\n")
        
        test_table = f"{self.test_database}.{self.test_table_prefix}_create_test"
        
        # Test 5.1: Overwrite data
        try:
            data = [(100, 'Updated_Alice', 999.99, datetime.now())]
            columns = ['id', 'name', 'value', 'created_at']
            df = self.spark.createDataFrame(data, columns)
            
            # Save row count before
            count_before = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {test_table}").collect()[0].cnt
            
            df.write.mode('overwrite').insertInto(test_table)
            
            # Verify after
            count_after = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {test_table}").collect()[0].cnt
            
            self._add_result(
                "Overwrite Table Data",
                "Update Permissions",
                "UPDATE",
                "PASS",
                f"Successfully overwrote table. Rows before: {count_before}, after: {count_after}"
            )
        except Exception as e:
            self._add_result(
                "Overwrite Table Data",
                "Update Permissions",
                "UPDATE",
                "FAIL",
                f"Cannot overwrite data: {str(e)}",
                traceback.format_exc()
            )
        
        # Test 5.2: Add column (ALTER TABLE)
        try:
            alter_sql = f"ALTER TABLE {test_table} ADD COLUMNS (new_column STRING)"
            self.spark.sql(alter_sql)
            
            # Verify column was added
            desc = self.spark.sql(f"DESCRIBE {test_table}").collect()
            col_names = [row.col_name for row in desc]
            
            if 'new_column' in col_names:
                self._add_result(
                    "Add Column to Table",
                    "Update Permissions",
                    "ALTER",
                    "PASS",
                    "Successfully added new column to table"
                )
            else:
                self._add_result(
                    "Add Column to Table",
                    "Update Permissions",
                    "ALTER",
                    "FAIL",
                    "ALTER TABLE command executed but column not found"
                )
        except Exception as e:
            self._add_result(
                "Add Column to Table",
                "Update Permissions",
                "ALTER",
                "FAIL",
                f"Cannot add column: {str(e)}",
                traceback.format_exc()
            )
        
        # Test 5.3: Rename table
        try:
            old_name = f"{self.test_database}.{self.test_table_prefix}_create_test"
            new_name = f"{self.test_database}.{self.test_table_prefix}_renamed"
            
            rename_sql = f"ALTER TABLE {old_name} RENAME TO {new_name}"
            self.spark.sql(rename_sql)
            
            # Verify
            tables = self.spark.sql(f"SHOW TABLES IN {self.test_database}").collect()
            table_names = [row.tableName for row in tables]
            
            if f"{self.test_table_prefix}_renamed" in table_names:
                self._add_result(
                    "Rename Table",
                    "Update Permissions",
                    "ALTER",
                    "PASS",
                    f"Successfully renamed table to '{new_name}'"
                )
                
                # Rename back for cleanup
                self.spark.sql(f"ALTER TABLE {new_name} RENAME TO {old_name}")
            else:
                self._add_result(
                    "Rename Table",
                    "Update Permissions",
                    "ALTER",
                    "FAIL",
                    "Table rename command executed but new name not found"
                )
        except Exception as e:
            self._add_result(
                "Rename Table",
                "Update Permissions",
                "ALTER",
                "FAIL",
                f"Cannot rename table: {str(e)}",
                traceback.format_exc()
            )
        
        return True
    
    # ========== TEST 6: DELETE PERMISSIONS ==========
    def test_delete_permissions(self):
        """Test 6: Verify DELETE permissions"""
        print("\n" + "="*80)
        print("TEST CATEGORY 6: DELETE PERMISSIONS")
        print("="*80 + "\n")
        
        # Test 6.1: TRUNCATE table
        try:
            test_table = f"{self.test_database}.{self.test_table_prefix}_create_test"
            
            # Get count before
            count_before = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {test_table}").collect()[0].cnt
            
            truncate_sql = f"TRUNCATE TABLE {test_table}"
            self.spark.sql(truncate_sql)
            
            # Get count after
            count_after = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {test_table}").collect()[0].cnt
            
            if count_after == 0:
                self._add_result(
                    "Truncate Table (Delete All Data)",
                    "Delete Permissions",
                    "TRUNCATE",
                    "PASS",
                    f"Successfully truncated table. Rows before: {count_before}, after: {count_after}"
                )
            else:
                self._add_result(
                    "Truncate Table (Delete All Data)",
                    "Delete Permissions",
                    "TRUNCATE",
                    "FAIL",
                    f"Table not fully truncated. Still has {count_after} rows"
                )
        except Exception as e:
            self._add_result(
                "Truncate Table (Delete All Data)",
                "Delete Permissions",
                "TRUNCATE",
                "FAIL",
                f"Cannot truncate table: {str(e)}",
                traceback.format_exc()
            )
        
        # Test 6.2: DROP table
        try:
            test_table = f"{self.test_database}.{self.test_table_prefix}_create_test"
            drop_sql = f"DROP TABLE IF EXISTS {test_table}"
            self.spark.sql(drop_sql)
            
            # Verify table is gone
            tables = self.spark.sql(f"SHOW TABLES IN {self.test_database}").collect()
            table_names = [row.tableName for row in tables]
            
            if f"{self.test_table_prefix}_create_test" not in table_names:
                self._add_result(
                    "Drop Table",
                    "Delete Permissions",
                    "DROP",
                    "PASS",
                    f"Successfully dropped table '{test_table}'"
                )
            else:
                self._add_result(
                    "Drop Table",
                    "Delete Permissions",
                    "DROP",
                    "FAIL",
                    "DROP TABLE executed but table still exists"
                )
        except Exception as e:
            self._add_result(
                "Drop Table",
                "Delete Permissions",
                "DROP",
                "FAIL",
                f"Cannot drop table: {str(e)}",
                traceback.format_exc()
            )
        
        # Test 6.3: Drop partitioned table
        try:
            partitioned_table = f"{self.test_database}.{self.test_table_prefix}_partitioned"
            drop_sql = f"DROP TABLE IF EXISTS {partitioned_table}"
            self.spark.sql(drop_sql)
            self._add_result(
                "Drop Partitioned Table",
                "Delete Permissions",
                "DROP",
                "PASS",
                "Successfully dropped partitioned table"
            )
        except Exception as e:
            self._add_result(
                "Drop Partitioned Table",
                "Delete Permissions",
                "DROP",
                "FAIL",
                f"Cannot drop partitioned table: {str(e)}",
                traceback.format_exc()
            )
        
        # Test 6.4: Drop external table
        try:
            external_table = f"{self.test_database}.{self.test_table_prefix}_external"
            drop_sql = f"DROP TABLE IF EXISTS {external_table}"
            self.spark.sql(drop_sql)
            self._add_result(
                "Drop External Table",
                "Delete Permissions",
                "DROP",
                "PASS",
                "Successfully dropped external table"
            )
        except Exception as e:
            self._add_result(
                "Drop External Table",
                "Delete Permissions",
                "DROP",
                "FAIL",
                f"Cannot drop external table: {str(e)}",
                traceback.format_exc()
            )
        
        return True
    
    # ========== RUN ALL TESTS ==========
    def run_all_tests(self):
        """Execute all test categories"""
        print(f"\nStarting comprehensive permission tests at {datetime.now()}\n")
        
        # Run tests in sequence
        if self.test_database_access():
            self.test_create_permissions()
            self.test_insert_permissions()
            self.test_read_permissions()
            self.test_update_permissions()
            self.test_delete_permissions()
        else:
            print("\n❌ Database access test failed. Skipping remaining tests.")
        
        self.test_end_time = datetime.now()
        self.generate_report()
    
    # ========== GENERATE REPORTS ==========
    def generate_report(self):
        """Generate comprehensive test report"""
        print("\n" + "="*80)
        print("TEST EXECUTION SUMMARY")
        print("="*80 + "\n")
        
        # Create DataFrame from results
        df_results = pd.DataFrame(self.test_results)
        
        # Calculate statistics
        total_tests = len(self.test_results)
        passed_tests = len(df_results[df_results['Status'] == 'PASS'])
        failed_tests = len(df_results[df_results['Status'] == 'FAIL'])
        warning_tests = len(df_results[df_results['Status'] == 'WARNING'])
        
        pass_rate = (passed_tests / total_tests * 100) if total_tests > 0 else 0
        
        # Print summary
        print(f"User: {self.current_user}")
        print(f"Database: {self.test_database}")
        print(f"Start Time: {self.test_start_time}")
        print(f"End Time: {self.test_end_time}")
        print(f"Duration: {self.test_end_time - self.test_start_time}")
        print()
        print(f"Total Tests: {total_tests}")
        print(f"✓ Passed: {passed_tests}")
        print(f"✗ Failed: {failed_tests}")
        print(f"⚠ Warnings: {warning_tests}")
        print(f"Pass Rate: {pass_rate:.1f}%")
        print()
        
        # Overall status
        if failed_tests == 0:
            print("🎉 OVERALL STATUS: ALL TESTS PASSED ✓")
        else:
            print(f"⚠️  OVERALL STATUS: {failed_tests} TEST(S) FAILED")
        
        print("\n" + "="*80)
        print("DETAILED TEST RESULTS")
        print("="*80 + "\n")
        
        # Display results table
        display(df_results[['Test #', 'Test Name', 'Category', 'Operation', 'Status', 'Message']])
        
        # Category breakdown
        print("\n" + "="*80)
        print("RESULTS BY CATEGORY")
        print("="*80 + "\n")
        
        category_summary = df_results.groupby(['Category', 'Status']).size().unstack(fill_value=0)
        display(category_summary)
        
        # Manager Checklist
        self.generate_manager_checklist(df_results, passed_tests, failed_tests, pass_rate)
        
        return df_results
    
    def generate_manager_checklist(self, df_results, passed, failed, pass_rate):
        """Generate a manager-ready checklist"""
        print("\n" + "="*80)
        print("MANAGER CHECKLIST - PERMISSION VERIFICATION")
        print("="*80 + "\n")
        
        checklist_data = []
        
        # Database Access
        db_tests = df_results[df_results['Category'] == 'Database Access']
        db_status = "✓ PASS" if all(db_tests['Status'] == 'PASS') else "✗ FAIL"
        checklist_data.append({
            'Permission Category': 'Database Access',
            'Status': db_status,
            'Tests Passed': f"{len(db_tests[db_tests['Status']=='PASS'])}/{len(db_tests)}",
            'Required For': 'Basic connectivity'
        })
        
        # Create
        create_tests = df_results[df_results['Category'] == 'Create Permissions']
        create_status = "✓ PASS" if all(create_tests['Status'] == 'PASS') else "✗ FAIL"
        checklist_data.append({
            'Permission Category': 'CREATE Tables',
            'Status': create_status,
            'Tests Passed': f"{len(create_tests[create_tests['Status']=='PASS'])}/{len(create_tests)}",
            'Required For': 'Creating new tables/schemas'
        })
        
        # Write/Insert
        write_tests = df_results[df_results['Category'] == 'Write Permissions']
        write_status = "✓ PASS" if all(write_tests['Status'] == 'PASS') else "✗ FAIL"
        checklist_data.append({
            'Permission Category': 'INSERT/WRITE Data',
            'Status': write_status,
            'Tests Passed': f"{len(write_tests[write_tests['Status']=='PASS'])}/{len(write_tests)}",
            'Required For': 'Loading data into tables'
        })
        
        # Read
        read_tests = df_results[df_results['Category'] == 'Read Permissions']
        read_status = "✓ PASS" if all(read_tests['Status'] == 'PASS') else "✗ FAIL"
        checklist_data.append({
            'Permission Category': 'SELECT/READ Data',
            'Status': read_status,
            'Tests Passed': f"{len(read_tests[read_tests['Status']=='PASS'])}/{len(read_tests)}",
            'Required For': 'Querying and analyzing data'
        })
        
        # Update
        update_tests = df_results[df_results['Category'] == 'Update Permissions']
        update_status = "✓ PASS" if all(update_tests['Status'] == 'PASS') else "✗ FAIL"
        checklist_data.append({
            'Permission Category': 'UPDATE/ALTER Tables',
            'Status': update_status,
            'Tests Passed': f"{len(update_tests[update_tests['Status']=='PASS'])}/{len(update_tests)}",
            'Required For': 'Modifying table structure/data'
        })
        
        # Delete
        delete_tests = df_results[df_results['Category'] == 'Delete Permissions']
        delete_status = "✓ PASS" if all(delete_tests['Status'] == 'PASS') else "✗ FAIL"
        checklist_data.append({
            'Permission Category': 'DELETE/DROP Tables',
            'Status': delete_status,
            'Tests Passed': f"{len(delete_tests[delete_tests['Status']=='PASS'])}/{len(delete_tests)}",
            'Required For': 'Removing tables/data'
        })
        
        df_checklist = pd.DataFrame(checklist_data)
        display(df_checklist)
        
        print("\n" + "="*80)
        print("EXECUTIVE SUMMARY FOR MANAGEMENT")
        print("="*80 + "\n")
        
        print(f"""
Test Execution Summary:
-----------------------
User Tested: {self.current_user}
Database: {self.test_database}
Execution Date: {self.test_start_time.strftime("%Y-%m-%d %H:%M")}

Overall Results:
- Total Test Cases: {passed + failed}
- Tests Passed: {passed}
- Tests Failed: {failed}
- Success Rate: {pass_rate:.1f}%

Conclusion:
-----------
""")
        
        if failed == 0:
            print(f"""✓ ALL PERMISSION TESTS PASSED

The user '{self.current_user}' has been verified to have complete CRUD 
(Create, Read, Update, Delete) permissions on the test database '{self.test_database}'.

The user can:
• Access and list databases
• Create new tables (standard, partitioned, external)
• Insert and write data via SQL and DataFrame APIs
• Read and query data with full access
• Modify table structures and data
• Delete and drop tables as needed

RECOMMENDATION: User is cleared for production database access with similar permissions.
""")
        else:
            print(f"""⚠️  SOME PERMISSION TESTS FAILED

The user '{self.current_user}' does not have complete permissions on '{self.test_database}'.

Failed Tests: {failed}
Success Rate: {pass_rate:.1f}%

RECOMMENDATION: Review failed test details above and grant missing permissions before 
                proceeding to production access.

Failed Categories:
""")
            failed_categories = df_results[df_results['Status'] == 'FAIL']['Category'].unique()
            for cat in failed_categories:
                failed_in_cat = len(df_results[(df_results['Category'] == cat) & (df_results['Status'] == 'FAIL')])
                print(f"  • {cat}: {failed_in_cat} test(s) failed")
        
        print("\n" + "="*80)


# ========== USAGE INSTRUCTIONS ==========
def print_usage_instructions():
    """Print instructions for using the test suite"""
    instructions = """
╔══════════════════════════════════════════════════════════════════════════════╗
║                HADOOP PERMISSIONS TESTING SUITE - USAGE GUIDE                ║
╚══════════════════════════════════════════════════════════════════════════════╝

PREREQUISITES:
--------------
1. Active SparkSession (variable name: spark)
2. Access to a NON-PRODUCTION test database
3. Permissions to create/modify/delete test tables

BASIC USAGE:
------------
# Step 1: Create your SparkSession (if not already created)
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("permission_test").getOrCreate()

# Step 2: Initialize the tester with your test database
tester = HadoopPermissionTester(
    spark=spark,
    test_database="your_test_database_name"  # ⚠️  NON-PRODUCTION ONLY!
)

# Step 3: Run all tests
tester.run_all_tests()

# Step 4: Get results DataFrame for further analysis
results_df = tester.generate_report()

EXAMPLE:
--------
# Test on a specific database
tester = HadoopPermissionTester(spark, "dev_sandbox_db")
tester.run_all_tests()

WHAT GETS TESTED:
-----------------
✓ Database Access (3 tests)
  - List databases
  - Switch to test database
  - List tables

✓ CREATE Permissions (3 tests)
  - Create standard table
  - Create partitioned table
  - Create external table

✓ INSERT/WRITE Permissions (3 tests)
  - Insert via SQL
  - Insert via DataFrame
  - Insert into partitioned table

✓ READ Permissions (4 tests)
  - SELECT query
  - Aggregation query
  - DataFrame API read
  - Metadata access

✓ UPDATE Permissions (3 tests)
  - Overwrite data
  - Add column (ALTER)
  - Rename table

✓ DELETE Permissions (4 tests)
  - TRUNCATE table
  - DROP table
  - DROP partitioned table
  - DROP external table

OUTPUTS:
--------
1. Real-time test execution feedback with ✓/✗ symbols
2. Detailed results table with all test outcomes
3. Category-wise breakdown
4. Manager-ready checklist
5. Executive summary with recommendations

IMPORTANT NOTES:
----------------
⚠️  This will CREATE, MODIFY, and DELETE test data
⚠️  Use ONLY on non-production test databases
⚠️  Test tables will be named: {database}.permission_test_*
⚠️  Failed tests will show error details for troubleshooting

For questions or issues, contact your Hadoop administrator.
"""
    print(instructions)


if __name__ == "__main__":
    print_usage_instructions()
