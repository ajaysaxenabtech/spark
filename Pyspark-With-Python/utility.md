

```python


# File: run_analysis.py or Jupyter notebook cell

from spark_session_builder import get_optimal_spark_session

# Initialize Spark for a heavy workload
spark = get_optimal_spark_session(queue_type="heavy", app_name="BigData_ETL_Job")

# Use Spark as usual
df = spark.read.csv("/path/to/data.csv", header=True, inferSchema=True)
df.show()


```

