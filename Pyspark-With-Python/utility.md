

```

def get_spark(queue: str = "default"):
    from pyspark.sql import SparkSession
    import os

    # Minimal configs to reduce startup time and maximize dynamic scaling
    try:
        spark = (
            SparkSession.builder
            .master("yarn")
            .appName("aFLD_Analysis")
            .enableHiveSupport()
            .config("spark.dynamicAllocation.enabled", "true")
            .config("spark.shuffle.service.enabled", "true")
            .config("spark.executor.cores", "2")  # Light config to start fast
            .config("spark.executor.memory", "2g")
            .config("spark.dynamicAllocation.minExecutors", "1")
            .config("spark.dynamicAllocation.maxExecutors", "50")
            .config("spark.dynamicAllocation.executorIdleTimeout", "20s")
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.yarn.queue", queue)
            .getOrCreate()
        )
        print("✅ Spark session started")
        return spark
    except Exception as e:
        print(f"❌ Spark session failed: {e}")
        return None

