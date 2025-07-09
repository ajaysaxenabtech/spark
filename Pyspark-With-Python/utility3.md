
You don’t actually need to guess what your queue’s slice is—YARN exposes it in three easy ways:

---

## 1. Via the ResourceManager Web UI

1. Go to your RM’s scheduler page (e.g. `http://<resourcemanager>:8088/cluster/scheduler`).
2. Click the little ▶︎ next to **root** to drill into each child queue (ESG, ESGP1, ESGP2, default, etc.).
3. In each row you’ll see three bars (Capacity – Used – Max Capacity) and the exact % values when you hover.

---

## 2. With the `yarn` CLI

Just run:

```bash
yarn rmadmin -getQueueInfo root.<YOUR_QUEUE_NAME>
```

You’ll get JSON like:

```json
{
  "queueName":"root.ESGP1",
  "capacity":"20.0",        # % of total cluster
  "usedCapacity":"5.3",     # % currently in use
  "maxCapacity":"25.0",     # % hard cap
  …  
}
```

---

## 3. Via the REST API

Hit the scheduler endpoint and parse out your queue:

```python
import requests

r = requests.get("http://<resourcemanager>:8088/ws/v1/cluster/scheduler")
data = r.json()["scheduler"]["schedulerInfo"]["queues"]["queue"]  # list of top-level
for q in data:
    if q["queueName"]=="root.ESGP1":
        print(q["capacity"], q["usedCapacity"], q["maxCapacity"])
```

You’ll see exactly “capacity” / “usedCapacity” / “maxCapacity” in percent.

---

**🎯 TL;DR**
**Quickest** is to expand the tree in the RM UI and hover over the bars—no CLI or code required.
