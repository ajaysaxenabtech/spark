

---
```
def get_spark(queue: str = "default") -> "SparkSession":
    """
    Initialize a SparkSession on YARN with minimal client-side configs,
    letting cluster-level spark-defaults.conf handle dynamic allocation,
    Kryo, adaptive shuffle, etc.
    """
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder
            .master("yarn")
            .appName("aFLD_Analysis")
            # Pick your queue (default, ESG, ESGP1, etc.)
            .config("spark.yarn.queue", queue)
            # Lean overrides — everything else is managed server-side
            .config("spark.executor.memory", "16g")
            .config("spark.executor.cores", "4")
            .config("spark.driver.memory",  "4g")
            # Disable client-side dynamic allocation handshake
            .config("spark.dynamicAllocation.enabled", "false")
            .config("spark.shuffle.service.enabled",    "false")
            .getOrCreate()
    )

# ─── Example ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    spark = get_spark("ESG")
    print(f"Spark v{spark.version} ⏱️ session ready")



---



```