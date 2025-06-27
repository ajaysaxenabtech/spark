

```

def get_spark(queue: str = None):
    import os, sys, pandas as pd, numpy as np
    import logging, time
    from IPython.display import display, HTML
    from pyspark.sql import SparkSession
    from pyspark import SparkContext, SparkConf

    # Set Pandas display options for notebooks
    display(HTML("<style>.container{width:100% !important;}</style>"))
    display(HTML("<style>.CodeMirror {white-space: pre-wrap !important;}</style>"))
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', 0)

    # Set environment variables (customize paths as needed)
    os.environ.update({
        "SPARK_HOME": "/opt/cloudera/parcels/CDH/lib/spark",
        "HADOOP_CONF_DIR": "/etc/hadoop/conf",
        "PYLIB": "/opt/cloudera/parcels/CDH/lib/spark/python/lib",
        "PYSPARK_PYTHON": "/opt/python_hsbc/env/esg/versions/3.6.10/bin/python3.6",
        "PYSPARK_DRIVER_PYTHON": "/opt/python_hsbc/env/esg/versions/3.6.10/bin/python3.6",
        "SPARK_MAJOR_VERSION": "2",
        "JAVA_HOME": "/usr/java/zulu8.78.0.20-sa-jdk8.0.412-linux_x64/",
        "LD_LIBRARY_PATH": "/usr/java/zulu8.78.0.20-sa-jdk8.0.412-linux_x64/lib/amd64/server",
        "ARROW_LIBHDFS_DIR": "/opt/cloudera/parcels/CDH/lib64",
        "KRB5CCNAME": f"/tmp/krb5cc_{os.geteuid()}"
    })

    sys.path.insert(0, f"{os.environ['PYLIB']}/py4j-0.10.7-src.zip")
    sys.path.insert(0, f"{os.environ['SPARK_HOME']}/python/lib/pyspark.zip")

    # Queue mapping logic
    if queue is None or queue.lower() == "d":
        yarn_queue = "default"
    elif queue == "e1":
        yarn_queue = "ESGP1"
    else:
        yarn_queue = queue

    # Resource detection
    page_size = os.sysconf("SC_PAGE_SIZE")
    num_pages = os.sysconf("SC_PHYS_PAGES")
    total_memory_gb = (page_size * num_pages) / (1024 ** 3)
    total_cores = os.cpu_count()

    # Dynamic resource limits
    max_cluster_memory_gb = 180
    executor_memory_gb = min(int(total_memory_gb * 0.6) // 2, 80)
    driver_memory_gb = min(int(total_memory_gb * 0.2), 40)
    executor_cores = min((total_cores * 60) // 100 // 2, 8)
    driver_cores = min((total_cores * 20) // 100, 6)

    print(f"Executor: {executor_memory_gb}g x {executor_cores} cores | Driver: {driver_memory_gb}g x {driver_cores} cores")

    # SparkSession builder
    try:
        spark = (
            SparkSession.builder
            .master("yarn")
            .appName("aFLD_Analysis")
            .enableHiveSupport()
            .config("spark.dynamicAllocation.enabled", "true")
            .config("spark.shuffle.service.enabled", "true")
            .config("spark.dynamicAllocation.minExecutors", "10")
            .config("spark.dynamicAllocation.maxExecutors", "100")
            .config("spark.dynamicAllocation.executorIdleTimeout", "60s")
            .config("spark.executor.cores", executor_cores)
            .config("spark.executor.memory", f"{executor_memory_gb}g")
            .config("spark.yarn.executor.memoryOverhead", "4g")
            .config("spark.driver.cores", driver_cores)
            .config("spark.driver.memory", f"{driver_memory_gb}g")
            .config("spark.driver.maxResultSize", "2g")
            .config("spark.memory.fraction", "0.8")
            .config("spark.memory.storageFraction", "0.3")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryoserializer.buffer", "64m")
            .config("spark.kryoserializer.buffer.max", "512m")
            .config("spark.broadcastTimeout", "900")
            .config("spark.sql.autoBroadcastJoinThreshold", "-1")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "128MB")
            .config("spark.speculation", "true")
            .config("spark.yarn.maxAppAttempts", "3")
            .config("spark.yarn.queue", yarn_queue)
            .config("spark.sql.shuffle.partitions", "600")
            .getOrCreate()
        )

        print("✅ Spark session initialized")
        return spark

    except Exception as e:
        print(f"❌ Failed to initialize Spark session on queue '{queue}': {e}")
        return None

