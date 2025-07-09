You’ve got three easy ways to wire in the `spark-excel` connector:

---

## 1. When you launch `pyspark` or `spark-shell`

```bash
pyspark \
  --packages com.crealytics:spark-excel_2.12:0.13.5 \
  --conf spark.executor.extraClassPath=com.crealytics:spark-excel_2.12:0.13.5
```

Or, for a one-liner in Linux/macOS:

```bash
export SPARK_JARS_PACKAGES=com.crealytics:spark-excel_2.12:0.13.5
export SPARK_CONF_DIR=/path/to/your/conf
pyspark
```

*(You’d put the `spark.executor.extraClassPath` into your `spark-defaults.conf` there.)*

---

## 2. Via `spark-submit`

```bash
spark-submit \
  --packages com.crealytics:spark-excel_2.12:0.13.5 \
  --conf spark.executor.extraClassPath=com.crealytics:spark-excel_2.12:0.13.5 \
  your_script.py
```

---

## 3. Directly in your Python code (notebook or script)

If you can’t restart the shell, just inject the JAR at session-build time:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
      .appName("ReadExcel")
      .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5")
      .config("spark.executor.extraClassPath", "com.crealytics:spark-excel_2.12:0.13.5")
      .getOrCreate()
)
```

Now you can do:

```python
df = (
  spark.read
    .format("com.crealytics.spark.excel")
    .option("dataAddress", "'Sheet1'!A1")
    .option("useHeader", "true")
    .load("hdfs://…/ESG_dwf_PM_report_07JUL2025.xlsx")
)
```

---

**Final take:** if you control your shell or cluster configs, go with **Option 1** or **2**. If you need it on-the-fly in a notebook, use **Option 3**.
