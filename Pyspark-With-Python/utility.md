
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
    .appName("FLD_Analysis_aj") \
    .enableHiveSupport() \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.shuffle.service.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "1") \
    .config("spark.dynamicAllocation.maxExecutors", "30") \
    .config("spark.dynamicAllocation.executorIdleTimeout", "60s") \
    .config("spark.executor.cores", "5") \
    .config("spark.executor.memory", "10g") \
    .config("spark.yarn.executor.memoryOverhead", "2g") \
    .config("spark.driver.cores", "5") \
    .config("spark.driver.memory", "10g") \
    .config("spark.memory.fraction", "0.6") \
    .config("spark.memory.storageFraction", "0.5") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.autoBroadcastJoinThreshold", "50MB") \
    .config("spark.speculation", "true") \
    .config("spark.yarn.maxAppAttempts", "5") \
    .config("spark.yarn.queue", "root.root.ESGP1") \
    .getOrCreate()

print("✅ Spark session initialized.")

pd.set_option('display.max_columns', None)

```

---






 