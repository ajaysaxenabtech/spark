

```
def get_spark_opt(queue: str = "default",
                  min_exec: int = 1,
                  max_exec: int = 200,
                  exec_cores: int = 4,
                  exec_mem_gb: int = 8,
                  driver_mem_gb: int = 4):

    from pyspark.sql import SparkSession

    # ----- Env glue kept short -----
    import os, sys
    SPARK_HOME = "/opt/cloudera/parcels/CDH/lib/spark"
    HADOOP_CONF_DIR = "/etc/hadoop/conf"
    os.environ.update({
        "SPARK_HOME": SPARK_HOME,
        "HADOOP_CONF_DIR": HADOOP_CONF_DIR,
        "PYSPARK_PYTHON": "/usr/bin/python3",
        "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3",
    })
    sys.path.insert(0, f"{SPARK_HOME}/python/lib/py4j-src.zip")
    sys.path.insert(0, f"{SPARK_HOME}/python/lib/pyspark.zip")

    # ----- Session builder -----
    spark = (
        SparkSession.builder
        .master("yarn")
        .appName("FaLD_Analysis")
        .enableHiveSupport()
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", str(min_exec))
        .config("spark.dynamicAllocation.maxExecutors", str(max_exec))
        .config("spark.executor.cores", str(exec_cores))
        .config("spark.executor.memory", f"{exec_mem_gb}g")
        .config("spark.driver.memory", f"{driver_mem_gb}g")
        .config("spark.sql.shuffle.partitions", "256")
        .config("spark.yarn.queue", queue)
        .getOrCreate()
    )

    return spark


