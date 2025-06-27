

```

# ------------------------------------------------------------------------
#  Spark bootstrap for JupyterHub  ─ secure, multi-tenant YARN
# ------------------------------------------------------------------------
import os, sys
from typing import Optional, Dict
from pyspark.sql import SparkSession

# 💡 Edit these only if your cluster doesn’t already set them correctly
_DEFAULT_ENV = {
    "SPARK_HOME"        : "/opt/cloudera/parcels/CDH/lib/spark",
    "HADOOP_CONF_DIR"   : "/etc/hadoop/conf",
    # Pick the site-approved Python for PySpark
    "PYSPARK_PYTHON"    : "/opt/python_hsbc/env/esg/versions/3.11/bin/python3.11",
    "PYSPARK_DRIVER_PYTHON": "/opt/python_hsbc/env/esg/versions/3.11/bin/python3.11",
}

def get_spark(
    # ---------- env ----------
    set_env: bool = False,                       # True → apply _DEFAULT_ENV
    env_dict: Optional[Dict[str, str]] = None,   # or supply your own map
    # ---------- queue / sizing ----------
    queue: Optional[str] = None,
    min_executors: int = 2,
    max_executors: int = 150,
    target_partitions: int = 400,
    executor_mem_pct: float = 0.45,
    driver_mem_pct: float   = 0.15,
    executor_cores_pct: float = 0.40,
    # ---------- security ----------
    kerberos_principal: Optional[str] = None,
    kerberos_keytab:   Optional[str] = None,
):
    """
    Return a host-aware SparkSession configured for enterprise YARN.
    Flip set_env=True to push the env vars in _DEFAULT_ENV, or pass env_dict
    to supply your own overrides.
    """
    # 🏗 Environment wiring -------------------------------------------------
    if set_env:
        for k, v in (env_dict or _DEFAULT_ENV).items():
            os.environ.setdefault(k, v)

    # 🔍 Host inspection ----------------------------------------------------
    page   = os.sysconf("SC_PAGE_SIZE")
    pages  = os.sysconf("SC_PHYS_PAGES")
    mem_gb = (page * pages) / 1024**3
    cores  = os.cpu_count()

    exe_mem = min(int(mem_gb * executor_mem_pct), 64)
    drv_mem = min(int(mem_gb * driver_mem_pct),   32)
    exe_crs = min(max(int(cores * executor_cores_pct), 1), 8)
    drv_crs = min(max(int(cores * 0.20), 1), 6)

    print(f"[Spark boot] host-RAM={mem_gb:.1f} GB | host-cores={cores}")
    print(f"  driver  : {drv_crs} core | {drv_mem} GB")
    print(f"  executor: {exe_crs} core | {exe_mem} GB × dyn({min_executors}-{max_executors})")

    # ⚙️  Session builder ---------------------------------------------------
    builder = (
        SparkSession.builder
        .master("yarn")
        .appName("Enterprise_JupyterHub_Session")
        .enableHiveSupport()
        # Dynamic allocation
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.shuffle.service.enabled",  "true")
        .config("spark.dynamicAllocation.minExecutors", min_executors)
        .config("spark.dynamicAllocation.maxExecutors", max_executors)
        .config("spark.dynamicAllocation.executorIdleTimeout", "60s")
        # Cores / memory
        .config("spark.executor.cores", exe_crs)
        .config("spark.executor.memory", f"{exe_mem}g")
        .config("spark.executor.memoryOverhead", max(int(exe_mem * 0.10), 2))
        .config("spark.driver.cores", drv_crs)
        .config("spark.driver.memory", f"{drv_mem}g")
        .config("spark.driver.maxResultSize", "4g")
        # AQE & shuffle
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
        .config("spark.sql.shuffle.partitions", target_partitions)
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # Serializer
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "1024m")
        # Misc
        .config("spark.speculation", "true")
        .config("spark.yarn.maxAppAttempts", "3")
        .config("spark.yarn.queue", queue or "default")
    )

    # 🔐 Kerberos hook (optional) ------------------------------------------
    if kerberos_principal and kerberos_keytab:
        builder = (
            builder
            .config("spark.yarn.principal", kerberos_principal)
            .config("spark.yarn.keytab", kerberos_keytab)
        )

    # 🚀 Create & return ----------------------------------------------------
    try:
        spark = builder.getOrCreate()
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        print("[Spark boot] session ready ✅")
        return spark
    except Exception as e:
        print(f"[Spark boot] ❌ failed: {e}")
        return None

# ------------------------------------------------------------------------
# Example
# ------------------------------------------------------------------------
# spark = get_spark(set_env=True, queue="research",
#                   kerberos_principal="ajay@CORP.COM",
#                   kerberos_keytab="/home/ajay/ajay.keytab")

