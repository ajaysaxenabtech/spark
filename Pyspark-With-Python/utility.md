

```
from pyspark.sql.functions import col, format_number, concat, lit
from pyspark.sql.types import DecimalType, StringType, IntegerType, TimestampType, DateType

# Custom transformation logic based on type and precision/scale
def transform_decimal(col_name, dtype):
    if dtype.precision == 38 and dtype.scale == 9:
        return format_number(col(col_name), 9).alias(col_name)
    elif dtype.precision == 18 and dtype.scale == 0:
        return format_number(col(col_name).cast("double"), 0).alias(col_name)
    else:
        return col(col_name)

def transform_string(col_name):
    return concat(lit(":"), col(col_name), lit("")).alias(col_name)

def transform_integer(col_name):
    return concat(lit("#"), col(col_name).cast("string"), lit("")).alias(col_name)

def transform_timestamp(col_name):
    return col(col_name).cast("string").alias(col_name)

def transform_date(col_name):
    return col(col_name).cast("string").alias(col_name)

# Define transformation dispatcher
def transform_column(col_field):
    dtype = col_field.dataType
    col_name = col_field.name
    if isinstance(dtype, DecimalType):
        return transform_decimal(col_name, dtype)
    elif isinstance(dtype, StringType):
        return transform_string(col_name)
    elif isinstance(dtype, IntegerType):
        return transform_integer(col_name)
    elif isinstance(dtype, TimestampType):
        return transform_timestamp(col_name)
    elif isinstance(dtype, DateType):
        return transform_date(col_name)
    else:
        return col(col_name)

# Apply transformations dynamically
formatted_cols = [transform_column(f) for f in kpi.schema.fields]

# Transformed DataFrame
kpi_transformed = kpi.select(formatted_cols)


