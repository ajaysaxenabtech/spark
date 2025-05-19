

---
```python
from pyspark.sql.functions import format_number, col
from pyspark.sql.types import DecimalType

formatted_cols = []

for f in kpi.schema.fields:
    if f.name in deci_cols and isinstance(f.dataType, DecimalType) and f.dataType.precision == 38 and f.dataType.scale == 9:
        formatted_cols.append(format_number(col(f.name), 9).alias(f.name))
    else:
        formatted_cols.append(col(f.name))


```