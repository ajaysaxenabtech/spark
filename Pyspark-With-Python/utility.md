
---
### increasing the working width of notebook

```python
from IPython.display import display,HTML
display(HTML("<style>.container{width:100% !important; }</style>"))

```

---

```python
from IPython.display import display, HTML
display(HTML("<style>.CodeMirror { white-space: pre-wrap !important; }</style>"))
```
---

To wrap text inside a Python code cell in Jupyter Notebook (on JupyterHub), you can use the following approaches:

### 1. **Enable Line Wrapping in Jupyter Notebook Settings**
   - Open a new cell and run:
     ```python
     from notebook.services.config import ConfigManager
     cm = ConfigManager()
     cm.update('notebook', {"CodeCell": {"cm_config": {"lineWrapping": True}}})
     ```
   - This will enable text wrapping inside code cells.

### 2. **Modify Jupyter Notebook Configuration**
   If you have access to JupyterHub's configuration settings, you can enable line wrapping permanently:
   - Open JupyterHub terminal and edit the `custom.js` file:
     ```bash
     nano ~/.jupyter/custom/custom.js
     ```
   - Add the following lines:
     ```js
     require(["notebook/js/codecell"], function (codecell) {
         codecell.CodeCell.options_default.cm_config.lineWrapping = true;
     });
     ```
   - Save the file and restart JupyterHub.

### 3. **Use Keyboard Shortcuts (Temporary Solution)**
   - Click inside a code cell and press:
     - `Ctrl + Shift + P` → Search for **"Toggle Line Wrap"** and enable it.

### 4. **Modify `custom.css` (Alternative UI Method)**
   - If your JupyterHub allows custom CSS, create a file:
     ```bash
     nano ~/.jupyter/custom/custom.css
     ```
   - Add:
     ```css
     .CodeMirror pre {
         white-space: pre-wrap !important;
         word-break: break-word !important;
     }
     ```
   - Save and restart JupyterHub.

---

```python

from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder \
    .master("yarn") \
    .appName("aFLD_Analysis") \
    .enableHiveSupport() \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.shuffle.service.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "1") \
    .config("spark.dynamicAllocation.maxExecutors", "70") \
    .config("spark.dynamicAllocation.executorIdleTimeout", "60s") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memory", "10g") \
    .config("spark.yarn.executor.memoryOverhead", "2g") \
    .config("spark.driver.cores", "5") \
    .config("spark.driver.memory", "10g") \
    .config("spark.memory.fraction", "0.6") \
    .config("spark.memory.storageFraction", "0.5") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.autoBroadcastJoinThreshold", "104857600") \
    .config("spark.speculation", "true") \
    .config("spark.yarn.maxAppAttempts", "5") \
    .config("spark.yarn.queue", "default") \
    # 🔽 AQE and shuffle optimization settings
    .config("spark.sql.shuffle.partitions", "300") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64MB") \
    .getOrCreate()


```

---
```python

from pyspark.sql import SparkSession
import pandas as pd

from pyspark.sql import SparkSession
import pandas as pd

try:
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("FLD_Analysis_aj_optimized") \
        .enableHiveSupport() \
        .config("spark.sql.shuffle.partitions", "2000") \  # Ideal: 2–3x total executors
        .config("spark.default.parallelism", "2000") \
        .config("spark.executor.instances", "125") \       # 125 executors × 8 cores = 1000 cores
        .config("spark.executor.cores", "8") \
        .config("spark.executor.memory", "16g") \
        .config("spark.yarn.executor.memoryOverhead", "4g") \
        .config("spark.driver.memory", "32g") \
        .config("spark.driver.cores", "4") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \  # Prevent OOM for large joins
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "2047m") \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.yarn.maxAppAttempts", "2") \
        .config("spark.yarn.queue", "default") \
        .getOrCreate()

except Exception as e:
    print(e)
else:
    print("✅ Spark session initialized for enterprise cluster (1000-core optimized).")

pd.set_option('display.max_columns', None)


```


---





 