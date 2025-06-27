

```

# ---------------------------------------------------------------------------
#  get_spark  –  enterprise JupyterHub bootstrap (CDH + Spark-2 + Zulu JDK 8)
# ---------------------------------------------------------------------------
import os, sys, glob, pathlib
from typing import Optional, Dict
from pyspark.sql import SparkSession

def get_spark(
    # ---------- env ----------
    set_env: bool = True,                               # export baseline env
    java_home: str = "/usr/java/zulu8.78.0.20-sa-jdk8.0.412-linux_x64",
    spark_home: str = "/opt/cloudera/parcels/CDH/lib/spark",
    python_bin: str = "/opt/python_hsbc/env/esg/versions/3.6.10/bin/python3.6",
    extra_env: Optional[Dict[str, str]] = None,         # override / add vars
    # ---------- YARN queue ----------
    queue: str = "default",                             # alias "d" handled
    # ---------- sizing (tweak as needed) ----------
    min_executors: int = 1,
    max_executors: int = 20,
    executor_mem_gb: int = 4,
    driver_mem_gb:   int = 4,
    executor_cores:  int = 2,
    driver_cores:    int = 2,
    target_partitions: int = 200,
    # ---------- security ----------
    kerberos_principal: Optional[str] = None,
    kerberos_keytab:   Optional[str] = None,
):
    """
    Return a SparkSession configured for Cloudera CDH (Spark 2) inside a
    JupyterHub container.  Safe defaults keep the JVM inside tight cgroups.
    """

    # 1️⃣  ENVIRONMENT ------------------------------------------------------
    if set_env:
        base = {
            "SPARK_HOME":            spark_home,
            "HADOOP_CONF_DIR":       "/etc/hadoop/conf",
            "PYSPARK_PYTHON":        python_bin,
            "PYSPARK_DRIVER_PYTHON": python_bin,
            "PYSPARK_SUBMIT_ARGS":   "--deploy-mode client pyspark-shell",
            "SPARK_MAJOR_VERSION":   "2",
            "JAVA_HOME":             java_home,
            "LD_LIBRARY_PATH":       f"{java_home}/lib/amd64/server",
            "KRB5CCNAME":            f"/tmp/krb5cc_{os.geteuid()}",
        }
        if extra_env:
            base.update(extra_env)
        for k, v in base.items():
            os.environ.setdefault(k, v)

        # sys.path tweaks (py4j + pyspark.zip)
        pylib = f"{spark_home}/python/lib"
        for pattern in [f"{pylib}/py4j-*-src.zip",
                        f"{spark_home}/python/lib/pyspark.zip"]:
            match = sorted(glob.glob(pattern))
            if match and match[0] not in sys.path:
                sys.path.insert(0, match[0])

    # sanity check Java
    assert pathlib.Path(os.environ["JAVA_HOME"], "bin/java").exists(), "JAVA_HOME incorrect"

    # 2️⃣  BANNER -----------------------------------------------------------
    print(f"[Spark boot] driver={driver_cores}c/{driver_mem_gb}g  "
          f"executor={executor_cores}c/{executor_mem_gb}g × dyn({min_executors}-{max_executors})")

    # queue alias
    yarn_queue = {"d": "default", "e1": "ESGPI"}.get(queue, queue)

    # 3️⃣  BUILDER ----------------------------------------------------------
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
        .config("spark.executor.cores", executor_cores)
        .config("spark.executor.memory", f"{executor_mem_gb}g")
        .config("spark.executor.memoryOverhead", max(int(executor_mem_gb * 0.10), 2))
        .config("spark.driver.cores", driver_cores)
        .config("spark.driver.memory", f"{driver_mem_gb}g")
        .config("spark.driver.maxResultSize", "2g")
        # shuffle
        .config("spark.sql.shuffle.partitions", target_partitions)
        # serializer
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "512m")
        # misc
        .config("spark.speculation", "true")
        .config("spark.yarn.maxAppAttempts", "3")
        .config("spark.yarn.queue", yarn_queue)
    )

    if kerberos_principal and kerberos_keytab:
        builder = (
            builder
            .config("spark.yarn.principal", kerberos_principal)
            .config("spark.yarn.keytab", kerberos_keytab)
        )

    # 4️⃣  CREATE -----------------------------------------------------------
    try:
        spark = builder.getOrCreate()
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        print("[Spark boot] session ready ✅")
        return spark
    except Exception as e:
        print(f"[Spark boot] ❌ failed: {e}")
        return None

# ---------------------------------------------------------------------------
# Example (fire it up):
# ---------------------------------------------------------------------------
# spark = get_spark()                     # default queue, safe sizing
# spark = get_spark(queue="e1")           # alias to ESGPI queue
# spark = get_spark(executor_mem_gb=8,
#                   driver_mem_gb=8,
#                   executor_cores=4,
#                   driver_cores=4)

