Here are three ways to get that XLSX off HDFS and into a DataFrame:

|                            Option                            |                         Pros                        | Cons                                                          |
| :----------------------------------------------------------: | :-------------------------------------------------: | :------------------------------------------------------------ |
|          **1. Copy to local, then `pd.read_excel`**          |         Super-simple, no extra Python deps.         | Extra I/O step, slogs your code.                              |
|        **2. Stream via an HDFS client and `BytesIO`**        |        Keeps it all in Python; no temp files.       | You’ll need `hdfs` (or `pyarrow.fs`) and a bit more plumbing. |
| **3. Read it natively with Spark + the Spark-Excel package** | Fully distributed, plugs right into your Spark job. | Requires adding the `com.crealytics:spark-excel` connector.   |

---

### 1. Copy locally, then use Pandas

```bash
hdfs dfs -copyToLocal /apps/hive/warehouse/esg/internal/dwf/2025-07-07/ESG_dwf_PM_report_07JUL2025.xlsx /tmp/ESG_PM.xlsx
```

```python
import pandas as pd
df = pd.read_excel("/tmp/ESG_PM.xlsx", engine="openpyxl")
```

---

### 2. Stream from HDFS into Pandas via `BytesIO`

```python
from hdfs import InsecureClient
from io import BytesIO
client = InsecureClient("http://<namenode-host>:50070")
with client.read(dwf_pm_source_input) as reader:
    data = reader.read()
df = pd.read_excel(BytesIO(data), engine="openpyxl")
```

---

### 3. (My pick) Use Spark-Excel for a zero-copy Spark DataFrame

1. Add the connector when you start Spark:

   ```bash
   pyspark \
     --packages com.crealytics:spark-excel_2.12:0.13.5 \
     --conf spark.executor.extraClassPath=com.crealytics:spark-excel_2.12:0.13.5
   ```
2. Then in Python:

   ```python
   df_pm = (
     spark.read
       .format("com.crealytics.spark.excel")
       .option("dataAddress", "'Sheet1'!A1")
       .option("useHeader", "true")
       .option("treatEmptyValuesAsNulls", "true")
       .option("inferSchema", "true")
       .load(dwf_pm_source_input)
   )
   ```

---

**Final recommendation**: go with **Option 3**—it’s the smoothest if you’re already in Spark land. You’ll avoid round-trips to local storage, and you get a full Spark DataFrame you can immediately write to Parquet or join with other tables.
