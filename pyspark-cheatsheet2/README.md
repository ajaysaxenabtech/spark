# 🐍 📄 PySpark Cheat Sheet

A quick reference guide to the most commonly used patterns and functions in PySpark SQL.

#### Table of Contents
- [Quickstart](#quickstart)
- [Basics](#basics)
- [Common Patterns](#common-patterns)
    - [Importing Functions & Types](#importing-functions--types)
    - [Filtering](#filtering)
    - [Regex](#using-regex)
    - [Joins](#joins)
    - [Column Operations](#column-operations)
    - [Casting & Coalescing Null Values & Duplicates](#casting--coalescing-null-values--duplicates)
- [String Operations](#string-operations)
    - [String Filters](#string-filters)
    - [String Functions](#string-functions)
- [Number Operations](#number-operations)
- [Date & Timestamp Operations](#date--timestamp-operations)
- [Array Operations](#array-operations)
- [Struct Operations](#struct-operations)
- [Aggregation Operations](#aggregation-operations)
- [Advanced Operations](#advanced-operations)
    - [Repartitioning](#repartitioning)
    - [UDFs (User Defined Functions](#udfs-user-defined-functions)
- [Useful Functions / Tranformations](#useful-functions--transformations)

If you can't find what you're looking for, check out the [PySpark Official Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.StructType.html) and add it here!

## Crucial

In PySpark, to access a specific column's value for each row in a DataFrame, you typically use **row objects**. This is usually done after collecting or converting the DataFrame into an RDD or a list of rows. Here's how you can access a column's value using the `row.column_name` syntax:

### Steps to Access `row.column_name`
1. **Collect the DataFrame as Rows:**
   Use the `.collect()` method to bring the data from the Spark DataFrame to the driver as a list of Row objects.

   ```python
   rows = df.collect()  # Collects DataFrame rows as a list of Row objects
   ```

2. **Access Column Value by Name:**
   You can then access the value of a specific column for a given row using the `row.column_name` syntax.

   ```python
   for row in rows:
       print(row.column_name)  # Replace 'column_name' with the actual column name
   ```
3. **converting the row values in a list**
    
   ```
    [row.column_name for row in df.select("column_name").collect()]
   ```


## Quickstart

Install on macOS:

```bash
brew install apache-spark && pip install pyspark
```

Create your first DataFrame:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# I/O options: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/io.html
df = spark.read.csv('/path/to/your/input/file')
```

## Basics

```python
# Show a preview
df.show()

# Show complete values
df.show(5,truncate=False)

# Show in vertical format
df.show(5,vertical=True)

# Show preview of first / last n rows
df.head(5)
df.tail(5)

# Show preview as JSON (WARNING: in-memory)
df = df.limit(10) # optional
print(json.dumps([row.asDict(recursive=True) for row in df.collect()], indent=2))

# Limit actual DataFrame to n rows (non-deterministic)
df = df.limit(5)

# Get columns
df.columns

# Get columns + column types
df.dtypes

# Get schema
df.schema

# Get row count
df.count()

# Get column count
len(df.columns)

# Write output to disk
df.write.csv('/path/to/your/output/file')
df.write.mode("overwrite").csv('/path/to/your/output/file')
df.coalesce(1).write.mode('overwrite').csv('/path/to/output/folder', header=True)
df.repartition(1).write.mode('overwrite').csv('/path/to/output/folder', header=True)




# Get results (WARNING: in-memory) as list of PySpark Rows
df = df.collect()

# Get results (WARNING: in-memory) as list of Python dicts
dicts = [row.asDict(recursive=True) for row in df.collect()]

# Convert (WARNING: in-memory) to Pandas DataFrame
df = df.toPandas()
```

## Common Patterns

#### Importing Functions & Types

```python
# Easily reference these as F.my_function() and T.my_type() below
from pyspark.sql import functions as F, types as T
```

#### Filtering

```python
# Filter on equals condition
df = df.filter(df.is_adult == 'Y')

# Filter on >, <, >=, <= condition
df = df.filter(df.age > 25)

# Multiple conditions require parentheses around each condition
df = df.filter((df.age > 25) & (df.is_adult == 'Y'))

# Compare against a list of allowed values
df = df.filter(col('first_name').isin([3, 4, 7]))

# Sort results
df = df.orderBy(df.age.asc()))
df = df.orderBy(df.age.desc()))
```

### Using Regex

The `.rlike()` function in PySpark is a column method used to check if a string column matches a specified regular expression (regex). It returns a boolean column indicating whether each value satisfies the regex pattern.

Here are various cases and examples where `.rlike()` can be applied:

---

1. **Simple Matching**
   Check if a string contains a specific substring.
   ```python
   from pyspark.sql import SparkSession
   from pyspark.sql.functions import col

   spark = SparkSession.builder.appName("rlike_examples").getOrCreate()

   data = [("apple",), ("banana",), ("cherry",), ("grape",)]
   df = spark.createDataFrame(data, ["fruit"])

   # Check if the column 'fruit' contains the letter 'a'
   df.filter(col("fruit").rlike("a")).show()
   ```
   **Result**:
   ```
   +------+
   | fruit|
   +------+
   | apple|
   |banana|
   | grape|
   +------+
   ```

---

2. **Pattern Matching Using Wildcards**
   Use regex patterns like `^` (start), `$` (end), or `.*` (wildcard).
   ```python
   # Check if the string starts with 'b'
   df.filter(col("fruit").rlike("^b")).show()

   # Check if the string ends with 'e'
   df.filter(col("fruit").rlike("e$")).show()
   ```

---

3. **Match Multiple Patterns**
   Match any of several patterns using the `|` operator.
   ```python
   # Match fruits that start with 'a' or 'b'
   df.filter(col("fruit").rlike("^a|^b")).show()
   ```
   **Result**:
   ```
   +------+
   | fruit|
   +------+
   | apple|
   |banana|
   +------+
   ```

---

4. **Case-Insensitive Matching**
   Use regex flags (e.g., `(?i)` for case-insensitive matching).
   ```python
   data = [("APPLE",), ("BaNaNa",), ("Cherry",), ("GRAPE",)]
   df = spark.createDataFrame(data, ["fruit"])

   # Match case-insensitively for 'apple'
   df.filter(col("fruit").rlike("(?i)^apple$")).show()
   ```
   **Result**:
   ```
   +------+
   | fruit|
   +------+
   | APPLE|
   +------+
   ```

---

5. **Matching Digits**
   Use regex to match numeric patterns.
   ```python
   data = [("123abc",), ("456def",), ("no_numbers",), ("789ghi",)]
   df = spark.createDataFrame(data, ["text"])

   # Match rows with any digits
   df.filter(col("text").rlike("\\d")).show()

   # Match rows starting with digits
   df.filter(col("text").rlike("^\\d+")).show()
   ```

---

6. **Match Special Characters**
   Escape special characters if they need to be matched literally.
   ```python
   data = [("hello.",), ("world!",), ("test$",), ("123?",)]
   df = spark.createDataFrame(data, ["text"])

   # Match rows containing a period (.)
   df.filter(col("text").rlike("\\.")).show()
   ```
   **Result**:
   ```
   +------+
   |  text|
   +------+
   |hello.|
   +------+
   ```

---

7. **Custom Length Validation**
   Check for strings of a specific length using quantifiers.
   ```python
   data = [("abc",), ("abcd",), ("abcdef",)]
   df = spark.createDataFrame(data, ["text"])

   # Match strings of exactly 4 characters
   df.filter(col("text").rlike("^.{4}$")).show()
   ```
   **Result**:
   ```
   +----+
   |text|
   +----+
   |abcd|
   +----+
   ```

---

8. **Extracting Patterns**
   Combine `.rlike()` with conditional logic to extract matched rows.
   ```python
   from pyspark.sql.functions import when

   df = df.withColumn("has_digits", when(col("text").rlike("\\d+"), "Yes").otherwise("No"))
   df.show()
   ```
   **Result**:
   ```
   +----------+-----------+
   |      text|has_digits|
   +----------+-----------+
   |    123abc|        Yes|
   |    456def|        Yes|
   |no_numbers|         No|
   |    789ghi|        Yes|
   +----------+-----------+
   ```

---

9. **Exclude Matches**
   Use `.rlike()` with `~` to negate the condition.
   ```python
   # Exclude rows with any digits
   df.filter(~col("text").rlike("\\d")).show()
   ```

---

10. **Combination of Conditions**
   Combine `.rlike()` with multiple conditions using logical operators (`&`, `|`).
   ```python
   # Match rows with digits and containing 'abc'
   df.filter((col("text").rlike("\\d")) & (col("text").rlike("abc"))).show()
   ```

---

Summary of Regex Patterns for `.rlike()`:
| Pattern         | Description                        |
|------------------|------------------------------------|
| `^pattern`       | Matches the start of the string   |
| `pattern$`       | Matches the end of the string     |
| `.*`             | Matches zero or more characters   |
| `\\d`            | Matches any digit (0–9)          |
| `\\D`            | Matches any non-digit            |
| `[abc]`          | Matches 'a', 'b', or 'c'         |
| `[^abc]`         | Matches any character except 'a', 'b', or 'c' |
| `(a|b)`          | Matches either 'a' or 'b'        |
| `{n}`            | Matches exactly n repetitions    |
| `{n,}`           | Matches n or more repetitions    |
| `{n,m}`          | Matches between n and m repetitions |

---

#### Joins

```python
# Left join in another dataset
df = df.join(person_lookup_table, 'person_id', 'left')

# Match on different columns in left & right datasets
df = df.join(other_table, df.id == other_table.person_id, 'left')

# Match on multiple columns
df = df.join(other_table, ['first_name', 'last_name'], 'left')

# Join two columns from different tables
# Add index column to each DataFrame
df1_with_index = df1.rdd.zipWithIndex().toDF(["col1", "index1"])
df2_with_index = df2.rdd.zipWithIndex().toDF(["col2", "index2"])

# Perform inner join on the index columns
joined_df = df1_with_index.join(df2_with_index, df1_with_index.index1 == df2_with_index.index2).select("col1", "col2")

```

#### Column Operations

```python
# Add a new static column
df = df.withColumn('status', F.lit('PASS'))

# Construct a new dynamic column
df = df.withColumn('full_name', F.when(
    (df.fname.isNotNull() & df.lname.isNotNull()), F.concat(df.fname, df.lname)
).otherwise(F.lit('N/A'))

# Pick which columns to keep, optionally rename some
df = df.select(
    'name',
    'age',
    F.col('dob').alias('date_of_birth'),
)

# Remove columns
df = df.drop('mod_dt', 'mod_username')

# Rename a column
df = df.withColumnRenamed('dob', 'date_of_birth')

# Keep all the columns which also occur in another dataset
df = df.select(*(F.col(c) for c in df2.columns))

# Batch Rename/Clean Columns
for col in df.columns:
    df = df.withColumnRenamed(col, col.lower().replace(' ', '_').replace('-', '_'))
```

#### Casting & Coalescing Null Values & Duplicates

```python
# Cast a column to a different type
df = df.withColumn('price', df.price.cast(T.DoubleType()))

# Replace all nulls with a specific value
df = df.fillna({
    'first_name': 'Tom',
    'age': 0,
})

# Take the first value that is not null
df = df.withColumn('last_name', F.coalesce(df.last_name, df.surname, F.lit('N/A')))

# Drop duplicate rows in a dataset (distinct)
df = df.dropDuplicates() # or
df = df.distinct()

# Drop duplicate rows, but consider only specific columns
df = df.dropDuplicates(['name', 'height'])

# Replace empty strings with null (leave out subset keyword arg to replace in all columns)
df = df.replace({"": None}, subset=["name"])

# Convert Python/PySpark/NumPy NaN operator to null
df = df.replace(float("nan"), None)
```

## String Operations

#### String Filters

```python
# Contains - col.contains(string)
df = df.filter(df.name.contains('o'))

# Starts With - col.startswith(string)
df = df.filter(df.name.startswith('Al'))

# Ends With - col.endswith(string)
df = df.filter(df.name.endswith('ice'))

# Is Null - col.isNull()
df = df.filter(df.is_adult.isNull())

# Is Not Null - col.isNotNull()
df = df.filter(df.first_name.isNotNull())

# Like - col.like(string_with_sql_wildcards)
df = df.filter(df.name.like('Al%'))

# Regex Like - col.rlike(regex)
df = df.filter(df.name.rlike('[A-Z]*ice$'))
df = df.filter(df.name.rlike("^[0-9]+(\.[0-9]+)?$"))

# Is In List - col.isin(*cols)
df = df.filter(df.name.isin('Bob', 'Mike'))
```

#### String Functions

```python
# Substring - col.substr(startPos, length)
df = df.withColumn('short_id', df.id.substr(0, 10))

# Trim - F.trim(col)
df = df.withColumn('name', F.trim(df.name))

# Left Pad - F.lpad(col, len, pad)
# Right Pad - F.rpad(col, len, pad)
df = df.withColumn('id', F.lpad('id', 4, '0'))

# Left Trim - F.ltrim(col)
# Right Trim - F.rtrim(col)
df = df.withColumn('id', F.ltrim('id'))

# Concatenate - F.concat(*cols)
df = df.withColumn('full_name', F.concat('fname', F.lit(' '), 'lname'))

# Concatenate with Separator/Delimiter - F.concat_ws(delimiter, *cols)
df = df.withColumn('full_name', F.concat_ws('-', 'fname', 'lname'))

# Regex Replace - F.regexp_replace(str, pattern, replacement)[source]
df = df.withColumn('id', F.regexp_replace(id, '0F1(.*)', '1F1-$1'))

# Regex Extract - F.regexp_extract(str, pattern, idx)
df = df.withColumn('id', F.regexp_extract(id, '[0-9]*', 0))
```

## Number Operations

```python
# Round - F.round(col, scale=0)
df = df.withColumn('price', F.round('price', 0))

# Floor - F.floor(col)
df = df.withColumn('price', F.floor('price'))

# Ceiling - F.ceil(col)
df = df.withColumn('price', F.ceil('price'))

# Absolute Value - F.abs(col)
df = df.withColumn('price', F.abs('price'))

# X raised to power Y – F.pow(x, y)
df = df.withColumn('exponential_growth', F.pow('x', 'y'))

# Select smallest value out of multiple columns – F.least(*cols)
df = df.withColumn('least', F.least('subtotal', 'total'))

# Select largest value out of multiple columns – F.greatest(*cols)
df = df.withColumn('greatest', F.greatest('subtotal', 'total'))
```

## Date & Timestamp Operations

```python
# Add a column with the current date
df = df.withColumn('current_date', F.current_date())

# Convert a string of known format to a date (excludes time information)
df = df.withColumn('date_of_birth', F.to_date('date_of_birth', 'yyyy-MM-dd'))

# Convert a string of known format to a timestamp (includes time information)
df = df.withColumn('time_of_birth', F.to_timestamp('time_of_birth', 'yyyy-MM-dd HH:mm:ss'))

# Get year from date:       F.year(col)
# Get month from date:      F.month(col)
# Get day from date:        F.dayofmonth(col)
# Get hour from date:       F.hour(col)
# Get minute from date:     F.minute(col)
# Get second from date:     F.second(col)
df = df.filter(F.year('date_of_birth') == F.lit('2017'))

# Add & subtract days
df = df.withColumn('three_days_after', F.date_add('date_of_birth', 3))
df = df.withColumn('three_days_before', F.date_sub('date_of_birth', 3))

# Add & Subtract months
df = df.withColumn('next_month', F.add_month('date_of_birth', 1))

# Get number of days between two dates
df = df.withColumn('days_between', F.datediff('start', 'end'))

# Get number of months between two dates
df = df.withColumn('months_between', F.months_between('start', 'end'))

# Keep only rows where date_of_birth is between 2017-05-10 and 2018-07-21
df = df.filter(
    (F.col('date_of_birth') >= F.lit('2017-05-10')) &
    (F.col('date_of_birth') <= F.lit('2018-07-21'))
)
```

## Array Operations

```python
# Column Array - F.array(*cols)
df = df.withColumn('full_name', F.array('fname', 'lname'))

# Empty Array - F.array(*cols)
df = df.withColumn('empty_array_column', F.array([]))

# Get element at index – col.getItem(n)
df = df.withColumn('first_element', F.col("my_array").getItem(0))

# Array Size/Length – F.size(col)
df = df.withColumn('array_length', F.size('my_array'))

# Flatten Array – F.flatten(col)
df = df.withColumn('flattened', F.flatten('my_array'))

# Unique/Distinct Elements – F.array_distinct(col)
df = df.withColumn('unique_elements', F.array_distinct('my_array'))

# Map over & transform array elements – F.transform(col, func: col -> col)
df = df.withColumn('elem_ids', F.transform(F.col('my_array'), lambda x: x.getField('id')))

# Return a row per array element – F.explode(col)
df = df.select(F.explode('my_array'))
```

## Struct Operations

```python
# Make a new Struct column (similar to Python's `dict()`) – F.struct(*cols)
df = df.withColumn('my_struct', F.struct(F.col('col_a'), F.col('col_b')))

# Get item from struct by key – col.getField(str)
df = df.withColumn('col_a', F.col('my_struct').getField('col_a'))
```


## Aggregation Operations

```python
# Row Count:                F.count()
# Sum of Rows in Group:     F.sum(*cols)
# Mean of Rows in Group:    F.mean(*cols)
# Max of Rows in Group:     F.max(*cols)
# Min of Rows in Group:     F.min(*cols)
# First Row in Group:       F.alias(*cols)
df = df.groupBy('gender').agg(F.max('age').alias('max_age_by_gender'))

# Collect a Set of all Rows in Group:       F.collect_set(col)
# Collect a List of all Rows in Group:      F.collect_list(col)
df = df.groupBy('age').agg(F.collect_set('name').alias('person_names'))

#Pivot the DataFrame
from pyspark.sql import functions as F

# Sample data (assuming df is your original DataFrame)
data = [
    (30.0876, 'E - TIER I'),
    (18.2044, 'C - TIER I'),
    (17.9439, 'G - TIER I'),
    (16.0077, 'A - TIER I'),
    (17.6065, 'C - TIER II'),
    (30.2500, 'E - TIER II'),
    (17.2176, 'G - TIER II')
]

# Creating DataFrame
df = spark.createDataFrame(data, ['nav', 'fund_name'])

# Pivoting the DataFrame
pivot_df = df.groupBy().pivot("fund_name").agg(F.first("nav"))

pivot("fund_name"): Transforms the values in fund_name into column names.
agg(F.first("nav")): Uses the first occurrence of nav as the value under each new column.
groupBy(): No grouping key is needed here, so we just use groupBy() with no arguments.
This will produce the output with each fund_name as a column and the respective nav values.


# Just take the lastest row for each combination (Window Functions)
from pyspark.sql import Window as W

window = W.partitionBy("first_name", "last_name").orderBy(F.desc("date"))
df = df.withColumn("row_number", F.row_number().over(window))
df = df.filter(F.col("row_number") == 1)
df = df.drop("row_number")
```

## Advanced Operations

#### .cache() Vs .persist()
```python
#.cache(): This is a shorthand for .persist() with the default storage level, which is MEMORY_ONLY. It stores the data in memory only.
df_cached = df.cache()
#.persist(): Allows you to specify different storage levels, such as memory and disk, memory only, disk only, etc.
from pyspark import StorageLevel
df_persisted = df.persist(StorageLevel.MEMORY_AND_DISK)
```

#### Repartitioning

```python
# Repartition – df.repartition(num_output_partitions)
df = df.repartition(1)
```

#### UDFs (User Defined Functions

```python
# Multiply each row's age column by two
times_two_udf = F.udf(lambda x: x * 2)
df = df.withColumn('age', times_two_udf(df.age))

# Randomly choose a value to use as a row's name
import random

random_name_udf = F.udf(lambda: random.choice(['Bob', 'Tom', 'Amy', 'Jenna']))
df = df.withColumn('name', random_name_udf())
```

## Useful Functions / Transformations

```python
def flatten(df: DataFrame, delimiter="_") -> DataFrame:
    '''
    Flatten nested struct columns in `df` by one level separated by `delimiter`, i.e.:

    df = [ {'a': {'b': 1, 'c': 2} } ]
    df = flatten(df, '_')
    -> [ {'a_b': 1, 'a_c': 2} ]
    '''
    flat_cols = [name for name, type in df.dtypes if not type.startswith("struct")]
    nested_cols = [name for name, type in df.dtypes if type.startswith("struct")]

    flat_df = df.select(
        flat_cols
        + [F.col(nc + "." + c).alias(nc + delimiter + c) for nc in nested_cols for c in df.select(nc + ".*").columns]
    )
    return flat_df


def lookup_and_replace(df1, df2, df1_key, df2_key, df2_value):
    '''
    Replace every value in `df1`'s `df1_key` column with the corresponding value
    `df2_value` from `df2` where `df1_key` matches `df2_key`

    df = lookup_and_replace(people, pay_codes, id, pay_code_id, pay_code_desc)
    '''
    return (
        df1
        .join(df2[[df2_key, df2_value]], df1[df1_key] == df2[df2_key], 'left')
        .withColumn(df1_key, F.coalesce(F.col(df2_value), F.col(df1_key)))
        .drop(df2_key)
        .drop(df2_value)
    )

```
