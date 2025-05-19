

---
```python
from pyspark.sql.types import DecimalType

decimal_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, DecimalType) and f.dataType.precision == 38 and f.dataType.scale == 9]

```