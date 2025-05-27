

```
path = "/apps/hive/warehouse/esg-external-disclosures/prod/cda/eu_taxonomy/eutax-counterparty-france-cda.parq"

# Define schema matching the actual data
from pyspark.sql.types import StructType, StructField, BinaryType

custom_schema = StructType([
    StructField("ed", BinaryType(), True),
    # Add other fields as needed
])

df = fr_spark.read.schema(custom_schema).load(path)

try:
    df.limit(5).show()  # Fixed method - was 'topands()' which doesn't exist
except Exception as e:
    print(e)