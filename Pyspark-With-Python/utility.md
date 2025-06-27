

```

# ------------------------------------------------------------------------
#  Spark bootstrap for JupyterHub (safe defaults)
# ------------------------------------------------------------------------
import os, sys
from typing import Optional, Dict
from pyspark.sql import SparkSession

_DEFAULT_ENV = {
    "SPARK_HOME"      : "/opt/cloudera/parcels/CDH/lib/spark",
    "HADOOP_CONF_DIR" : "/etc/hadoop/conf",
    "PYSPARK_PYTHON"  : "/opt/python_hsbc/env/esg/versions/3.11/bin/python3.11",
    "PYSPARK_DRIVER_PYTHON": "/opt/python_hsbc/env/esg/versions/3.11/bin/python3.11",
}

# 🔑 queue aliases restored
_QUEUE_ALIAS = {"d": "default", "e1": "ESGPI"}

def get_spark(
    set_env: bool = False,
    env_dict: Optional[Dict[str, str]] = None,
    queue: Optional[str] = None,
    # ---------- sizing (new, safer defaults) ----------
    min_executors: int = 1,
    max_executors: int = 50,
    target_partitions: int = 200,
    executor_mem_pct: float = 0.10,    # 10 % of host RAM
    driver_mem_pct: float   = 0.05,    # 5 % of host RAM
    executor_cores_pct: float = 0.25,  # 25 % of host cores
    # ---------- security ----------
    kerberos_principal: Optional[str] = None,
    kerberos_keytab:   Optional[str] = None,
):
    """Return a host-aware SparkSession on secure YARN (Jupyter-friendly)."""

    # 1️⃣ OPTIONAL ENV EXPORT ------------------------------------------------
    if set_env:
        for k, v in (env_dict or _DEFAULT_ENV).items():
            os.environ.setdefault(k, v)

    # 2️⃣ RESOURCE DETECTION (use cgroup if present) ------------------------
    def _cgroup_limit() -> Optional[int]:
        """Return cgroup mem limit in bytes or None."""
        try:
            with open("/sys/fs/cgroup/memory/memory.limit_in_bytes") as f:
                val = int(f.read().strip())
                # Ignore insanely large numbers that mean 'no limit'
                return val if val < 1 << 50 else None
        except (FileNotFoundError, ValueError):
            return None

    mem_bytes = _cgroup_limit() or (os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"))
    mem_gb    = mem_bytes / 1024**3
    cores     = os.cpu_count() or 1

    exe_mem = min(int(mem_gb * executor_mem_pct), 16)  # hard clamp 16 GB
    drv_mem = min(int(mem_gb * driver_mem_pct),   8)   # hard clamp  8 GB
    exe_crs = min(max(int(cores * executor_cores_pct), 1), 4)
    drv_crs = min(max(int(cores * 0.20), 1), 4)

    # 3️⃣ PRETTY BANNER ------------------------------------------------------
    print(f"[Spark boot] quota-RAM={mem_gb:.1f} GB | quota-cores={cores}")
    print(f"  driver  : {drv_crs} core | {drv_mem} GB")
    print(f"  executor: {exe_crs} core | {exe_mem} GB × dyn({min_executors}-{max_executors})")

    # 4️⃣ QUEUE RESOLUTION ---------------------------------------------------
    yarn_queue = _QUEUE_ALIAS.get(queue, queue) or "default"

    # 5️⃣ SPARK BUILDER ------------------------------------------------------
    builder = (
        SparkSession.builder
        .master("yarn")
        .appName("JupyterHub_Session")
        .enableHiveSupport()
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.shuffle.service.enabled",  "true")
        .config("spark.dynamicAllocation.minExecutors", min_executors)
        .config("spark.dynamicAllocation.maxExecutors", max_executors)
        .config("spark.dynamicAllocation.executorIdleTimeout", "60s")
        .config("spark.executor.cores", exe_crs)
        .config("spark.executor.memory", f"{exe_mem}g")
        .config("spark.executor.memoryOverhead", max(int(exe_mem * 0.10), 2))
        .config("spark.driver.cores", drv_crs)
        .config("spark.driver.memory", f"{drv_mem}g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
        .config("spark.sql.shuffle.partitions", target_partitions)
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "512m")
        .config("spark.speculation", "true")
        .config("spark.yarn.maxAppAttempts", "3")
        .config("spark.yarn.queue", yarn_queue)
    )

    # Kerberos hook
    if kerberos_principal and kerberos_keytab:
        builder = (
            builder
            .config("spark.yarn.principal", kerberos_principal)
            .config("spark.yarn.keytab", kerberos_keytab)
        )

    # 6️⃣ CREATE & RETURN ----------------------------------------------------
    try:
        spark = builder.getOrCreate()
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        print("[Spark boot] session ready ✅")
        return spark
    except Exception as e:
        print(f"[Spark boot] ❌ failed: {e}")
        return None

