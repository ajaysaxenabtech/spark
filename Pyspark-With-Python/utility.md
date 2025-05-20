Here is the structured **documentation** for your PySpark data transformation and export pipeline, as seen in the image.

---

## 📘 Documentation: PySpark Data Transformation for CSV Export

### Overview:

This pipeline is designed to prepare a PySpark DataFrame for clean and readable CSV export. It specifically:

1. Identifies and formats `DecimalType(38, 9)` columns to avoid scientific notation.
2. Encapsulates certain string fields with double quotes to preserve value integrity.
3. Exports the transformed data using a custom `egress_eut` function.

---

### 📌 Step 1: Identify Decimal(38, 9) Columns

```python
from pyspark.sql.types import DecimalType

deci_cols = [f.name for f in kpi.schema.fields 
             if isinstance(f.dataType, DecimalType) 
             and f.dataType.precision == 38 
             and f.dataType.scale == 9]
```

**Purpose**:
Filters all columns from the DataFrame `kpi` where the data type is exactly `DecimalType(38, 9)`. These are typically high-precision numeric fields.

---

### 📌 Step 2: Format Decimal(38, 9) Columns to String with 9 Decimal Places

```python
from pyspark.sql.functions import format_number, col

formatted_cols = []

for f in kpi.schema.fields:
    if f.name in deci_cols and isinstance(f.dataType, DecimalType) and f.dataType.precision == 38 and f.dataType.scale == 9:
        formatted_cols.append(format_number(col(f.name), 9).alias(f.name))
    else:
        formatted_cols.append(col(f.name))

kpi_original3 = kpi.select(formatted_cols)
```

**Purpose**:
Ensures that all decimal(38,9) values are exported with 9 decimal places and **without scientific notation** by using `format_number()`. Other columns are passed through without change.

---

### 📌 Step 3: Wrap Selected String Columns with Double Quotes

```python
from pyspark.sql.functions import concat, lit

str_col_list = ["iipaalt", "iarrsys"]

for col_name in str_col_list:
    kpi_original1 = kpi_original3.withColumn(
        col_name,
        concat(lit('"'), col(col_name), lit('"'))
    )
```

**Purpose**:
Prepares specific string columns (`iipaalt`, `iarrsys`) for CSV export by surrounding their values with double quotes to preserve delimiters or special characters inside values.

---

### 📌 Step 4: Final Assignment and Sample View

```python
kpi_original = kpi_original1
kpi_original1.limit(5).toPandas()
```

**Purpose**:

* Assigns the final transformed DataFrame.
* Previews the first 5 rows using `.toPandas()` (useful for Jupyter or notebook environments).

---

### 📌 Step 5: Export Using Custom Function

```python
from udf.egressEUT import egress_eut

egress_eut(kpi_original, "kpi_final1")
```

**Purpose**:
Uses a **custom egress utility** to export the final DataFrame, likely to HDFS or external storage in CSV format.

---

