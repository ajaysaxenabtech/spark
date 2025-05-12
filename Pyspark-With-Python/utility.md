
---

```python

import os
import shutil
from pyspark.sql import SparkSession

def get_optimal_spark_session(queue_type="default", app_name="Dynamic_Spark_App"):
    # Estimate total system memory (RAM)
    try:
        total_memory_bytes = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
        total_memory_gb = round(total_memory_bytes / (1024 ** 3)) - 2  # buffer for OS
    except:
        total_memory_gb = 16  # fallback

    # Estimate total cores
    try:
        total_cores = os.cpu_count() or 4
    except:
        total_cores = 4  # fallback

    executor_memory = f"{max(4, int(total_memory_gb * 0.6))}g"
    memory_overhead = f"{max(2, int(total_memory_gb * 0.15))}g"

    queue_map = {
        "default": "default",
        "heavy": "ESGP2",
        "light": "ESG"
    }
    selected_queue = queue_map.get(queue_type, "default")

    try:
        spark = SparkSession.builder \
            .master("yarn") \
            .appName(app_name) \
            .enableHiveSupport() \
            .config("spark.dynamicAllocation.enabled", "true") \
            .config("spark.shuffle.service.enabled", "true") \
            .config("spark.dynamicAllocation.minExecutors", "5") \
            .config("spark.dynamicAllocation.maxExecutors", "150") \
            .config("spark.executor.cores", str(min(5, total_cores))) \
            .config("spark.executor.memory", executor_memory) \
            .config("spark.yarn.executor.memoryOverhead", memory_overhead) \
            .config("spark.driver.cores", str(min(4, total_cores))) \
            .config("spark.driver.memory", executor_memory) \
            .config("spark.memory.fraction", "0.8") \
            .config("spark.memory.storageFraction", "0.3") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.broadcastTimeout", "900") \
            .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "128MB") \
            .config("spark.speculation", "true") \
            .config("spark.yarn.maxAppAttempts", "3") \
            .config("spark.yarn.queue", selected_queue) \
            .config("spark.sql.shuffle.partitions", "600") \
            .getOrCreate()

        print(f"Spark session initialized in queue '{selected_queue}' with memory = {executor_memory}")
        return spark

    except Exception as e:
        print("Spark initialization failed:", e)
        return None


```


---

```python


# File: run_analysis.py or Jupyter notebook cell

from spark_session_builder import get_optimal_spark_session

# Initialize Spark for a heavy workload
spark = get_optimal_spark_session(queue_type="heavy", app_name="BigData_ETL_Job")

# Use Spark as usual
df = spark.read.csv("/path/to/data.csv", header=True, inferSchema=True)
df.show()


```

