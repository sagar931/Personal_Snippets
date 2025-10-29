# Check your current username
import os
import getpass

print("Current User:", getpass.getuser())
print("User ID:", os.getuid())
print("Groups:", os.getgroups())

# Check if Kerberos is being used
import subprocess
try:
    result = subprocess.run(['klist'], capture_output=True, text=True)
    if result.returncode == 0:
        print("\n✓ Kerberos is ACTIVE:")
        print(result.stdout)
    else:
        print("\n✗ Kerberos not active (using simple authentication)")
except FileNotFoundError:
    print("\n✗ Kerberos not installed (using simple authentication)")

# Check Spark's reported user
print("\nSpark User:", spark.sparkContext.sparkUser())

# Check Hadoop authentication mode
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
auth_mode = hadoop_conf.get("hadoop.security.authentication")
print("Hadoop Auth Mode:", auth_mode)
