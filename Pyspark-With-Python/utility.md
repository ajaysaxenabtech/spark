

```

def get_spark(
        queue: str | None = None,
        min_executors: int = 1,
        max_executors: int = 100,
        target_partitions: int = 400,
        executor_mem_pct: float = 0.5,      # % of host RAM per executor
        driver_mem_pct: float   = 0.15,     # % of host RAM for driver
        executor_cores_pct: float = 0.5     # % of host cores per executor
    ):
    """
    Spin up a SparkSession on YARN with dynamic, host-aware sizing.
    Exposes the key levers so you can nudge perf without touching the guts.
    """
    # ---------- std lib ----------
    import os, sys, logging, time
    import numpy as np                # keep these
    import pandas as pd

    # ---------- env wiring ----------
    # only set what’s absolutely necessary; let cluster defaults win
    os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/CDH/lib/spark"
    os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
    os.environ["PYSPARK_PYTHON"] = "/opt/python_hsbc/env/esg/versions/3.11/bin/python3.11"

    # ---------- host introspection ----------
    page  = os.sysconf("SC_PAGE_SIZE")
    pages = os.sysconf("SC_PHYS_PAGES")
    mem_gb = (page * pages) / 1024**3
    cores  = os.cpu_count()

    # ceiling caps (safety nets, esp. in shared notebooks)
    exec_mem_gb = min(int(mem_gb * executor_mem_pct), 64)
    drv_mem_gb  = min(int(mem_gb * driver_mem_pct),   32)
    exec_cores  = min(max(int(cores * executor_cores_pct),1), 8)
    drv_cores   = min(max(int(cores * 0.2),1), 6)

    # nice-looking banner
    print(f"[Spark boot] host-RAM={mem_gb:.1f} GB, host-cores={cores}")
    print(f"  Driver  : {drv_cores} cores | {drv_mem_gb} GB")
    print(f"  Executor: {exec_cores} cores | {exec_mem_gb} GB × dyn({min_executors}-{max_executors})")

    # ---------- session ----------
    from pyspark.sql import SparkSession

    try:
        spark = (
            SparkSession.builder
            .master("yarn")
            .appName("aFLD_Analysis")
            .enableHiveSupport()
            # ---------- dynamic allocation ----------
            .config("spark.dynamicAllocation.enabled", "true")
            .config("spark.shuffle.service.enabled",  "true")
            .config("spark.dynamicAllocation.minExecutors", str(min_executors))
            .config("spark.dynamicAllocation.maxExecutors", str(max_executors))
            .config("spark.dynamicAllocation.executorIdleTimeout", "60s")
            # ---------- cores / memory ----------
            .config("spark.executor.cores",          exec_cores)
            .config("spark.executor.memory",        f"{exec_mem_gb}g")
            .config("spark.executor.memoryOverhead", max(int(exec_mem_gb*0.1), 2) )  # 10 %, ≥2 GB
            .config("spark.driver.cores",           drv_cores)
            .config("spark.driver.memory",         f"{drv_mem_gb}g")
            .config("spark.driver.maxResultSize",  "4g")
            # ---------- serializer ----------
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryoserializer.buffer.max", "1024m")
            # ---------- SQL / AQE ----------
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
            .config("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
            .config("spark.sql.shuffle.partitions", target_partitions)
            # ---------- misc ----------
            .config("spark.speculation", "true")
            .config("spark.yarn.queue", queue or "default")
            .getOrCreate()
        )
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")  # blazing pandas UDFs 🔥
        print("[Spark boot] session up 🟢")
        return spark

    except Exception as e:
        print(f"[Spark boot] failed: {e}")
        return None













# ------------------------------------------------------------------------
#  Spark bootstrap for JupyterHub  ─ tuned for secure, multi-tenant YARN
# ------------------------------------------------------------------------
from __future__ import annotations
import os, sys
from typing import Optional
from pyspark.sql import SparkSession

def get_spark(
    queue: Optional[str] = None,                # YARN queue (None → default)
    min_executors: int = 2,                     # dyn. allocation floor
    max_executors: int = 150,                   # dyn. allocation cap
    target_partitions: int = 400,               # default shuffle partitions
    executor_mem_pct: float = 0.45,             # % of host RAM per executor
    driver_mem_pct: float   = 0.15,             # % of host RAM for driver
    executor_cores_pct: float = 0.40,           # % of host cores per executor
    kerberos_principal: Optional[str] = None,   # e.g. "ajay@CORP.COM"
    kerberos_keytab:   Optional[str] = None,    # e.g. "/home/ajay/ajay.keytab"
):
    """Return a host-aware SparkSession configured for enterprise YARN."""
    # 🔍 Host inspection ---------------------------------------------------
    page  = os.sysconf("SC_PAGE_SIZE")
    pages = os.sysconf("SC_PHYS_PAGES")
    mem_gb  = (page * pages) / 1024**3
    cores   = os.cpu_count()

    exe_mem = min(int(mem_gb * executor_mem_pct), 64)
    drv_mem = min(int(mem_gb * driver_mem_pct),   32)
    exe_crs = min(max(int(cores * executor_cores_pct), 1), 8)
    drv_crs = min(max(int(cores * 0.20), 1), 6)

    print(f"[Spark boot] host-RAM={mem_gb:.1f} GB | host-cores={cores}")
    print(f"  driver  : {drv_crs} core | {drv_mem} GB")
    print(f"  executor: {exe_crs} core | {exe_mem} GB × dyn({min_executors}-{max_executors})")

    # ⚙️  Session builder --------------------------------------------------
    builder = (
        SparkSession.builder
        .master("yarn")
        .appName("Enterprise_JupyterHub_Session")
        .enableHiveSupport()                                # metastore hooked
        # --- Dynamic allocation (plays nice with multi-tenant clusters) ---
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.shuffle.service.enabled",  "true")
        .config("spark.dynamicAllocation.minExecutors", min_executors)
        .config("spark.dynamicAllocation.maxExecutors", max_executors)
        .config("spark.dynamicAllocation.executorIdleTimeout", "60s")
        # --- Core / memory ---
        .config("spark.executor.cores", exe_crs)
        .config("spark.executor.memory", f"{exe_mem}g")
        .config("spark.executor.memoryOverhead", max(int(exe_mem * 0.10), 2))
        .config("spark.driver.cores", drv_crs)
        .config("spark.driver.memory", f"{drv_mem}g")
        .config("spark.driver.maxResultSize", "4g")
        # --- AQE & shuffle ---
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
        .config("spark.sql.shuffle.partitions", target_partitions)
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # --- Serializer ---
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "1024m")
        # --- Speculation (safely on) ---
        .config("spark.speculation", "true")
        # --- YARN & queue ---
        .config("spark.yarn.maxAppAttempts", "3")
        .config("spark.yarn.queue", queue or "default")
    )

    # 🔐 Kerberos (optional) ----------------------------------------------
    if kerberos_principal and kerberos_keytab:
        builder = (
            builder
            .config("spark.yarn.principal", kerberos_principal)
            .config("spark.yarn.keytab", kerberos_keytab)
        )

    # 🚀 Create / return ---------------------------------------------------
    try:
        spark = builder.getOrCreate()
        # enable Arrow for snappy Pandas ↔︎ Spark transfers
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        print("[Spark boot] session ready ✅")
        return spark
    except Exception as e:
        print(f"[Spark boot] ❌ failed: {e}")
        return None

# ------------------------------------------------------------------------
# Example
# ------------------------------------------------------------------------
# spark = get_spark(queue="research", kerberos_principal="ajay@CORP.COM",
#                   kerberos_keytab="/home/ajay/ajay.keytab")

