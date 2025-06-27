

```

# ----------------------------------------------------------------------------------
#  Enterprise JupyterHub bootstrap  –  mirrors your original CDH / Spark-2 env setup
# ----------------------------------------------------------------------------------
import os, sys, glob
from typing import Optional, Dict
from pyspark.sql import SparkSession


# --------------------------------------------------------------------------
# 1. ENVIRONMENT – identical to the screenshot
# --------------------------------------------------------------------------
def _export_original_env() -> None:
    SPARK_HOME  = "/opt/cloudera/parcels/CDH/lib/spark"
    PYTHON_BIN  = "/opt/python_hsbc/env/esg/versions/3.6.10/bin/python3.6"
    JAVA_HOME   = "/usr/java/zulu8.78.0.20-sa-jdk8.0.412-linux_x64"

    base_env = {
        "SPARK_HOME"             : SPARK_HOME,
        "HADOOP_CONF_DIR"        : "/etc/hadoop/conf",
        "PYSPARK_PYTHON"         : PYTHON_BIN,
        "PYSPARK_DRIVER_PYTHON"  : PYTHON_BIN,
        "PYSPARK_SUBMIT_ARGS"    : "--deploy-mode client pyspark-shell",
        "SPARK_MAJOR_VERSION"    : "2",
        "JAVA_HOME"              : JAVA_HOME,
        "LD_LIBRARY_PATH"        : f"{JAVA_HOME}/lib/amd64/server",
        "ARROW_LIBHDFS_DIR"      : "/opt/cloudera/parcels/CDH/lib64",
        "KRB5CCNAME"             : f"/tmp/krb5cc_{os.geteuid()}",
    }

    # drive PYLIB off SPARK_HOME → same as your snippet
    base_env["PYLIB"] = f"{SPARK_HOME}/python/lib"

    for k, v in base_env.items():
        os.environ.setdefault(k, v)

    # replicate the extra sys.path inserts
    pylib = os.environ["PYLIB"]
    for pattern in [f"{pylib}/py4j-*-src.zip",
                    f"{os.environ['SPARK_HOME']}/python/lib/pyspark.zip"]:
        match = sorted(glob.glob(pattern))
        if match and match[0] not in sys.path:
            sys.path.insert(0, match[0])


_export_original_env()      # do it once when the cell runs


# --------------------------------------------------------------------------
# 2. RESOURCE-AWARE SparkSession builder
# --------------------------------------------------------------------------
_QUEUE_ALIAS = {"d": "default", "e1": "ESGPI"}   # your legacy shorthands

def get_spark(
    queue: Optional[str] = None,
    min_executors: int = 1,
    max_executors: int = 20,
    executor_mem_gb: int = 4,   # <= 4 GB so it fits typical JHub cgroup
    driver_mem_gb:   int = 4,   # 4 GB driver
    executor_cores:  int = 2,   # 2 vCPU / executor
    driver_cores:    int = 2,   # 2 vCPU driver
    target_partitions: int = 200,
    kerberos_principal: Optional[str] = None,
    kerberos_keytab:   Optional[str] = None,
):
    # ---- pretty banner ---------------------------------------------------
    print(f"[Spark boot] driver={driver_cores}c/{driver_mem_gb}g  "
          f"executor={executor_cores}c/{executor_mem_gb}g × dyn({min_executors}-{max_executors})")

    yarn_queue = _QUEUE_ALIAS.get(queue, queue) or "default"

    builder = (
        SparkSession.builder
        .master("yarn")
        .appName("JupyterHub_Session")
        .enableHiveSupport()
        # dynamic allocation
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.shuffle.service.enabled",  "true")
        .config("spark.dynamicAllocation.minExecutors", min_executors)
        .config("spark.dynamicAllocation.maxExecutors", max_executors)
        .config("spark.dynamicAllocation.executorIdleTimeout", "60s")
        # cores / memory
        .config("spark.executor.cores",           executor_cores)
        .config("spark.executor.memory",         f"{executor_mem_gb}g")
        .config("spark.executor.memoryOverhead", max(int(executor_mem_gb * 0.10), 2))
        .config("spark.driver.cores",            driver_cores)
        .config("spark.driver.memory",          f"{driver_mem_gb}g")
        .config("spark.driver.maxResultSize",    "2g")
        # shuffle / partitions
        .config("spark.sql.shuffle.partitions", target_partitions)
        # serializer
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "512m")
        # misc
        .config("spark.speculation", "true")
        .config("spark.yarn.maxAppAttempts", "3")
        .config("spark.yarn.queue", yarn_queue)
    )

    # kerberos hook (leave blank if your notebook already has a ticket)
    if kerberos_principal and kerberos_keytab:
        builder = (
            builder
            .config("spark.yarn.principal", kerberos_principal)
            .config("spark.yarn.keytab", kerberos_keytab)
        )

    # ---- create / return -------------------------------------------------
    try:
        spark = builder.getOrCreate()
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        print("[Spark boot] session ready ✅")
        return spark
    except Exception as e:
        print(f"[Spark boot] ❌ failed: {e}")
        return None


# --------------------------------------------------------------------------
# EXAMPLE
# --------------------------------------------------------------------------
# spark = get_spark(queue="d")           # <- “d” ⇒ “default”
# spark = get_spark(queue="ESGPI")       # <- explicit queue name

