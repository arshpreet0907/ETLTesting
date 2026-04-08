# PySpark Local Setup — Windows (Java + Python already installed)

## Step 1 — Verify Java

Open a terminal and confirm Java is accessible:

```
java -version
```

Expected output (example):
```
openjdk version "11.0.21" 2023-10-17
```

If this fails, Java is not on your PATH. Find your Java install location
(usually `C:\Program Files\Java\jdk-11.x.x` or `C:\Program Files\Eclipse Adoptium\...`)
and continue to the next step.

---

## Step 2 — Set JAVA_HOME (system environment variable)

1. Press `Win + S` → search "Edit the system environment variables" → Open
2. Click "Environment Variables..."
3. Under "System variables" click "New":
   - Variable name: `JAVA_HOME`
   - Variable value: your JDK path, e.g. `C:\Program Files\Java\jdk-11.0.21`
4. Find the `Path` variable in System variables → Edit → New → add:
   `%JAVA_HOME%\bin`
5. Click OK on all dialogs
6. **Open a new terminal** and verify:

```
echo %JAVA_HOME%
java -version
```

---

## Step 3 — Download winutils (Hadoop stub for Windows)

PySpark on Windows requires a minimal Hadoop native stub. Without it Spark crashes
with `Could not locate executable null\bin\winutils.exe`.

1. Create the folder: `C:\hadoop\bin`
2. Download `winutils.exe` and `hadoop.dll` for Hadoop 3.x from:
   https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.5/bin
   (download the raw files — right click → Save link as)
3. Place both files in `C:\hadoop\bin`

---

## Step 4 — Set HADOOP_HOME

Same path as Step 2 for JAVA_HOME:

1. System Environment Variables → New:
   - Variable name: `HADOOP_HOME`
   - Variable value: `C:\hadoop`
2. Add to Path: `%HADOOP_HOME%\bin`
3. Open a new terminal and verify:

```
echo %HADOOP_HOME%
```

---

## Step 5 — Create a Python virtual environment

Navigate to your project folder:

```
cd C:\Users\arshpreet.singh\PycharmProjects\TableVerifier
python -m venv venv
venv\Scripts\activate
```

You should see `(venv)` at the start of your prompt.

---

## Step 6 — Install Python dependencies

```
pip install pyspark==3.5.1
pip install openpyxl
pip install pyyaml
pip install pandas
pip install pymysql
pip install sqlalchemy
pip install jinja2
```

Verify PySpark installed:

```
python -c "import pyspark; print(pyspark.__version__)"
```

Expected: `3.5.1`

---

## Step 7 — Download JDBC driver JARs

Create a `jars/` folder in your project root.

### MySQL JDBC driver
Download from: https://dev.mysql.com/downloads/connector/j/
- Choose "Platform Independent" → download the ZIP
- Extract and copy `mysql-connector-j-8.x.x.jar` to your `jars/` folder

### Snowflake JDBC driver (only if using Snowflake target)
Download from: https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/
- Get the latest `snowflake-jdbc-3.x.x.jar`
- Copy to `jars/` folder

### Spark-Snowflake connector (only if using Snowflake target)
Download from: https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/
- Get the version matching your Spark (for Spark 3.5, use `spark-snowflake_2.12-2.15.x-spark_3.5.jar`)
- Copy to `jars/` folder

Your `jars/` folder should look like:
```
jars/
├── mysql-connector-j-8.3.0.jar
├── snowflake-jdbc-3.15.0.jar         (optional)
└── spark-snowflake_2.12-2.15.0-spark_3.5.jar  (optional)
```

---

## Step 8 — Quick smoke test

Create a file `test_spark.py` in your project root:

```python
import os
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-11.0.21"  # adjust to yours
os.environ["HADOOP_HOME"] = r"C:\hadoop"

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("SmokeTest") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

data = [("Alice", 30), ("Bob", 25)]
df = spark.createDataFrame(data, ["name", "age"])
df.show()

spark.stop()
print("PySpark is working correctly.")
```

Run it:

```
python test_spark.py
```

Expected output:
```
+-----+---+
| name|age|
+-----+---+
|Alice| 30|
|  Bob| 25|
+-----+---+
PySpark is working correctly.
```

---

## Step 9 — Test MySQL JDBC connectivity

Replace connection details with yours:

```python
import os
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-11.0.21"
os.environ["HADOOP_HOME"] = r"C:\hadoop"

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("JDBCTest") \
    .config("spark.ui.enabled", "false") \
    .config("spark.jars", r"jars/mysql-connector-j-8.3.0.jar") \
    .getOrCreate()

df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://YOUR_HOST:3306/YOUR_DB?useSSL=false&allowPublicKeyRetrieval=true") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("query", "SELECT 1 AS test_col") \
    .option("user", "YOUR_USER") \
    .option("password", "YOUR_PASSWORD") \
    .load()

df.show()
spark.stop()
```

---

## Step 10 — Project folder structure

Create the full folder scaffold:

```
mkdir config
mkdir input
mkdir rulebook
mkdir generated
mkdir output
mkdir jars
mkdir connections
mkdir utils
```

Copy the `etl_testing_context.md` file into the project root. You are now ready
to generate the individual framework files using an AI model.

---

## Common Issues on Windows

| Symptom | Cause | Fix |
|---|---|---|
| `Could not locate executable null\bin\winutils.exe` | HADOOP_HOME not set | Set HADOOP_HOME and restart terminal |
| `JAVA_HOME is not set` inside Spark log | JAVA_HOME not picked up | Set it in code before SparkSession (see spark_session.py) |
| `ClassNotFoundException: com.mysql.cj.jdbc.Driver` | JAR not loaded | Pass jar path in `.config("spark.jars", ...)` |
| `java.sql.SQLException: Access denied` | Wrong MySQL credentials or SSL | Add `?useSSL=false&allowPublicKeyRetrieval=true` to JDBC URL |
| Spark log floods the terminal | Default log level INFO | Add `spark.sparkContext.setLogLevel("WARN")` after building session |
| `Py4JJavaError` with `OutOfMemoryError` | Default JVM heap too small | Add `.config("spark.driver.memory", "2g")` to SparkSession builder |

---

## Summary of what is installed / configured

| Component | What | Where |
|---|---|---|
| Java 11 | JVM for PySpark | System PATH via JAVA_HOME |
| Hadoop winutils | Native stubs for Windows | C:\hadoop\bin |
| PySpark 3.5.1 | Core framework | Python venv |
| openpyxl | Excel sheet parsing | Python venv |
| PyYAML | Config file loading | Python venv |
| MySQL JDBC JAR | DB connectivity | jars/ |
| Snowflake JARs | DB connectivity (optional) | jars/ |
