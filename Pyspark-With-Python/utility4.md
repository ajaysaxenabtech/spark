

Here’s how to read that ESGP1 page, and what it means for your Spark settings:

| Metric                               | Value                                | Notes                                                                                    |
| ------------------------------------ | ------------------------------------ | ---------------------------------------------------------------------------------------- |
| **Absolute Configured Capacity**     | 40.0 %                               | That queue is allowed to use up to 40 % of the entire cluster’s resources.               |
| **Absolute Configured Max Capacity** | 50.0 %                               | Under pressure, it can burst up to 50 % of the cluster.                                  |
| **Effective Capacity**               | 9,414,748 MiB & 525 vCores (40.0 %)  | 40 % of cluster RAM = \~9.0 TiB, 40 % of cores = 525.  This is your “steady” limit.      |
| **Effective Max Capacity**           | 11,768,436 MiB & 657 vCores (50.0 %) | Up to \~11.2 TiB RAM and 657 cores if the cluster is idle enough to let you borrow more. |

---

### 🛠️ What to request in Spark

* **Driver + executors combined** must fit under **9 414 748 MiB** and **525 vCores** (or at most the 50 % burst ceiling if you really need it).
* So if you spin up, say, 8 executors with 4 cores each (32 cores) and 16 GiB memory each (8 × 16 = 128 GiB), you’re *way* under your queue’s limits—and YARN won’t choke on “Resource temporarily unavailable.”

```python
SparkSession.builder \
  .master("yarn") \
  .appName("aFLD") \
  .config("spark.yarn.queue", "ESGP1") \
  .config("spark.executor.instances", "8") \
  .config("spark.executor.cores", "4") \
  .config("spark.executor.memory", "16g") \
  .config("spark.driver.cores", "2") \
  .config("spark.driver.memory", "8g") \
  .getOrCreate()
```

Feel free to scale up (more executors, more cores, more RAM) up to **\~525 cores** and **\~9 414 748 MiB** of RAM—just keep the total below that 40 % line, and you won’t hit Errno 11 again.
