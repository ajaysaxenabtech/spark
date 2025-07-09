It looks like you’re trying to feed an HDFS-stored Parquet (or non-Excel) file into `pd.read_excel(…, engine="openpyxl")`, which will always bomb with a “not an Excel file” error. Here are your options:

1. **If your source is really Parquet** (which from the path it seems to be):

   ```python
   dwf_pm_source_df = spark.read.parquet(dwf_pm_source_input)
   ```

   then convert to Pandas if you need:

   ```python
   dwf_pm_source_pd = dwf_pm_source_df.toPandas()
   ```

2. **If it truly is an Excel file on HDFS**, stream it via PyArrow’s HDFS client:

   ```python
   from pyarrow import fs
   import pandas as pd

   hdfs = fs.HadoopFileSystem()
   with hdfs.open_input_file(dwf_pm_source_input) as f:
       dwf_pm_source_pd = pd.read_excel(f, engine="openpyxl")
   ```

3. **Copy it locally first**, then use Pandas normally:

   ```bash
   hadoop fs -copyToLocal hdfs://…/file.xlsx /tmp/file.xlsx
   ```

   ```python
   pd.read_excel("/tmp/file.xlsx", engine="openpyxl")
   ```

---

**Final answer (pick one):**
Use Spark’s Parquet reader instead of `pd.read_excel`:

```python
dwf_pm_source_df = spark.read.parquet(dwf_pm_source_input)
```
