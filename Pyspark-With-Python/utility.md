

```

def get_spark(queue: str | None = None):
    """
    Spin up a SparkSession that starts quickly on a laptop
    *or* scales on-cluster via YARN.
    """
    import os, math
    from pyspark.sql import SparkSession

    # ---- 1. Basic env (do this once, not every call) -----------------------
    os.environ.setdefault("SPARK_MAJOR_VERSION", "3")          # Spark 3.x
    os.environ.setdefault("JAVA_HOME", "/usr/java/default")    # cluster path
    if "SPARK_HOME" not in os.environ:
        os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/CDH/lib/spark"

    # ---- 2. Local resource probe ------------------------------------------
    ram_gb   = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / 1_073_741_824
    cores    = os.cpu_count()
    queue    = {"d": "default", "e1": "ESGP1"}.get(queue, queue or "default")

    # ---- 3. Friendly defaults (can be overridden by YARN) ------------------
    #   * 3-4 cores per executor  |  60-65 % of RAM goes to executors
    num_exec = max(2, cores // 4)
    ex_cores = min(4, max(1, cores // num_exec))
    ex_mem   = max(2, int(ram_gb * 0.65 / num_exec))          # GB
    drv_mem  = max(2, int(ram_gb * 0.15))                     # GB
    ovh_mem  = max(1, int(ex_mem * 0.10))                     # GB

    # ---- 4. Build the session ---------------------------------------------
    spark = (
        SparkSession.builder
        .master("yarn")
        .appName("aFLD_Analysis")
        # ---------- Dynamic allocation ----------
        .config("spark.dynamicAllocation.enabled",         "true")
        .config("spark.dynamicAllocation.initialExecutors", str(num_exec))
        .config("spark.dynamicAllocation.minExecutors",    "2")
        .config("spark.dynamicAllocation.maxExecutors",    "200")
        # ---------- Core / memory ----------
        .config("spark.executor.instances", str(num_exec))
        .config("spark.executor.cores",     str(ex_cores))
        .config("spark.executor.memory",    f"{ex_mem}g")
        .config("spark.executor.memoryOverhead", f"{ovh_mem}g")
        .config("spark.driver.memory",      f"{drv_mem}g")
        # ---------- Shuffle / AQE ----------
        .config("spark.sql.adaptive.enabled",              "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled","true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes","128MB")
        .config("spark.sql.shuffle.partitions",            str(num_exec * ex_cores * 2))
        # ---------- Serializer & Arrow ----------
        .config("spark.serializer",        "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.execution.arrow.pyspark.enabled","true")
        # ---------- Timeouts & misc ----------
        .config("spark.broadcastTimeout",   "600")
        .config("spark.speculation",        "true")
        .config("spark.yarn.queue",         queue)
        .getOrCreate()
    )

    print(f"🚀 Spark up ▶ executors={num_exec} | ex_mem={ex_mem}g | ex_cores={ex_cores}")
    return spark

