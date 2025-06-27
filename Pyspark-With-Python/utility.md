

```

# ---------------------------------------------------------------------------
#  get_spark  –  CDH / Spark-2 bootstrap with auto-Java detection
# ---------------------------------------------------------------------------
import os, sys, glob, pathlib, shutil
from typing import Optional, Dict, List
from pyspark.sql import SparkSession

# -- helper -----------------------------------------------------------------
def _find_java_candidates() -> List[str]:
    """Return a list of plausible JAVA_HOME paths on this node."""
    cands = set()

    # 1. what "java" on PATH resolves to
    java_bin = shutil.which("java")
    if java_bin:
        cands.add(str(pathlib.Path(java_bin).resolve().parent.parent))

    # 2. common distro & Cloudera locations
    for pattern in [
        "/usr/java/*",
        "/usr/lib/jvm/*",
        "/usr/java/default",
    ]:
        cands.update(glob.glob(pattern))

    # keep only ones that contain bin/java
    good = [
        p for p in cands
        if pathlib.Path(p, "bin/java").exists()
    ]
    return sorted(good)          # deterministic order


def get_spark(
    # ---------- env ----------
    set_env: bool = True,
    java_home: Optional[str] = None,              # auto if None
    spark_home: str = "/opt/cloudera/parcels/CDH/lib/spark",
    python_bin: str = "/opt/python_hsbc/env/esg/versions/3.6.10/bin/python3.6",
    extra_env: Optional[Dict[str, str]] = None,
    # ---------- YARN queue ----------
    queue: str = "default",                       # alias "d" handled
    # ---------- sizing ----------
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
    Start a SparkSession in a JupyterHub container on CDH (Spark 2).

    *Auto-detects* a Java 8 installation if `java_home` isn’t supplied.
    """

    # 0️⃣  pick JAVA_HOME ----------------------------------------------------
    if java_home is None:
        candidates = _find_java_candidates()
        if not candidates:
            raise RuntimeError(
                "Could not locate a JDK (bin/java not found in common paths). "
                "Pass java_home='…' explicitly or ask ops to install Java 8."
            )
        # prefer Zulu/OpenJDK 8 over newer versions
        java_home = next(
            (p for p in candidates if "8." in p or "jdk8" in p or "java-1.8" in p),
            candidates[0]
        )

    # 1️⃣  ENVIRONMENT ------------------------------------------------------
    if set_env:
        base = {
            "JAVA_HOME":             java_home,
            "SPARK_HOME":            spark_home,
            "HADOOP_CONF_DIR":       "/etc/hadoop/conf",
            "PYSPARK_PYTHON":        python_bin,
            "PYSPARK_DRIVER_PYTHON": python_bin,
            "PYSPARK_SUBMIT_ARGS":   "--deploy-mode client pyspark-shell",
            "SPARK_MAJOR_VERSION":   "2",
            "LD_LIBRARY_PATH":       f"{java_home}/lib/amd64/server",
            "KRB5CCNAME":            f"/tmp/krb5cc_{os.geteuid()}",
        }
        if extra_env:
            base.update(extra_env)
        for k, v in base.items():
            os.environ.setdefault(k, v)

        # sys.path tweaks
        pylib = f"{spark_home}/python/lib"
        for pattern in [f"{pylib}/py4j-*-src.zip", f"{spark_home}/python/lib/pyspark.zip"]:
            for path in glob.glob(pattern):
                if path not in sys.path:
                    sys.path.insert(0, path)

    # verify Java exists
    if not pathlib.Path(os.environ["JAVA_HOME"], "bin/java").exists():
        raise RuntimeError(f"JAVA_HOME points at {os.environ['JAVA_HOME']} but bin/java is missing")

    # 2️⃣  BANNER -----------------------------------------------------------
    print(f"[Spark boot] Java @ {os.environ['JAVA_HOME']}")
    print(f"[Spark boot] driver={driver_cores}c/{driver_mem_gb}g  "
          f"executor={executor_cores}c/{executor_mem_gb}g × dyn({min_executors}-{max_executors})")

    yarn_queue = {"d": "default", "e1": "ESGPI"}.get(queue, queue)

    # 3️⃣  BUILDER ----------------------------------------------------------
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
        .config("spark.executor.cores", executor_cores)
        .config("spark.executor.memory", f"{executor_mem_gb}g")
        .config("spark.executor.memoryOverhead", max(int(executor_mem_gb * 0.10), 2))
        .config("spark.driver.cores", driver_cores)
        .config("spark.driver.memory", f"{driver_mem_gb}g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.shuffle.partitions", target_partitions)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "512m")
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

    # 4️⃣  CREATE & RETURN --------------------------------------------------
    try:
        spark = builder.getOrCreate()
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        print("[Spark boot] session ready ✅")
        return spark
    except Exception as e:
        print(f"[Spark boot] ❌ failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Example usages
# ---------------------------------------------------------------------------
# spark = get_spark()                 # auto-detect Java, default queue
# spark = get_spark(queue="e1")       # send to ESGPI queue
# spark = get_spark(java_home="/usr/lib/jvm/java-1.8.0")        # manual path

