

```
from pyspark.sql.types import *

# Step 1: Load schema and modify sl_no to StringType
schema = spark.read.parquet(path).schema
# Remove sl_no if present
fields = [f for f in schema.fields if f.name != "sl_no"]
fields.append(StructField("sl_no", StringType(), True))
custom_schema = StructType(fields)

# Step 2: Read using the custom schema
df = spark.read.schema(custom_schema).parquet(path)

# Step 3: Cast sl_no to Long
from pyspark.sql.functions import col
df = df.withColumn("sl_no", col("sl_no").cast("long"))

# Step 4: Use as needed (drop duplicates, toPandas, etc.)
df = df.dropDuplicates(["sl_no"])
result = df.select(sorted(df.columns, reverse=True)).limit(5).toPandas()
print(result)
```