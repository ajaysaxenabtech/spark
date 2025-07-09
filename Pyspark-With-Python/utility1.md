Below are three ways you can kill that EAGAIN (Errno 11) when you spin up your SparkSession – pick one and run with it:

---

**1. Dial back your resource request**
Your auto‐detection logic is grabbing the full 32 cores and 755 GB of RAM on that node, then asking YARN for 10 executors × 8 cores each (plus a 6-core driver) – which the queue simply can’t satisfy. Instead, hard-code something more modest:

```python
spark = (
    SparkSession.builder
      .master("yarn")
      .appName("aFLD_Analysis")
      .enableHiveSupport()
      .config("spark.executor.instances", "4")        # only 4 executors
      .config("spark.executor.cores", "4")            # 4 cores each
      .config("spark.executor.memory", "16g")         # 16 GB each
      .config("spark.driver.cores", "2")
      .config("spark.driver.memory", "8g")
      .getOrCreate()
)
```

> **Why this works:** you’re staying well inside your queue’s capacity so YARN happily allocates containers instead of failing with EAGAIN.

---

**2. Tweak your dynamic-allocation minima**
If you still want dynamic scaling, don’t start with 10 executors by default. Drop that to, say, 2–3, so the very first allocation fits:

```python
.config("spark.dynamicAllocation.enabled",      "true")
.config("spark.dynamicAllocation.minExecutors","2")    # start small
.config("spark.dynamicAllocation.maxExecutors","20")
.config("spark.dynamicAllocation.executorIdleTimeout","60s")
```

> **Why this works:** smaller “floor” means your app can launch immediately; Spark will still grow up toward `maxExecutors` as needed.

---

**3. Query YARN for real cluster capacity**
Instead of reading local `/proc` values, hit the YARN REST API to discover how much memory / cores your queue actually has, and drive your config from those numbers. Rough outline:

```python
import requests

resp = requests.get("http://<resourcemanager>:8088/ws/v1/cluster/scheduler")
caps = resp.json()["scheduler"]["schedulerInfo"]["capacitiesByQueue"]["QUEUENAME"]
# parse caps["maximumCapacity"] etc., then compute your executor_memory/executor_cores
```

> **Why this works:** you no longer over-estimate; you adapt to whatever your org’s admins have locked down.

---

### 🎯 My pick: **Option 1**

For 90% of use-cases, the quickest win is to *just* lower your executors/drivers to something YARN can handle. Hard-coding a 4×4×16 GB setup gets you up and running instantly – you can always tweak later once you know what headroom your queue actually has.
