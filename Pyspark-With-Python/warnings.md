
### WARN window.Window.Exec: No Partition Defined for Window operation! Moving all data to a single partitings

1. Set Log Level to ERROR<br>
You can reduce the log level from WARN to ERROR. This way, only errors are shown, and warnings are suppressed.

```
spark.sparkContext.setLogLevel("ERROR")
```

2. Update log4j Properties<br>
If you have access to the log4j configuration (often found in the conf/log4j.properties file in your Spark setup), you can adjust the settings:

Add or modify the following line:
```
log4j.logger.org.apache.spark.sql.execution.window.WindowExec=ERROR
```

3. Programmatically Using spark-defaults.conf<br>
If you prefer not to modify the configuration files, you can set the log level programmatically when initializing your Spark session:
```
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.sql.execution.window.WindowExec", "ERROR") \
    .getOrCreate()
```

---

```powershell
Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;

public class Keyboard {
    [DllImport("user32.dll", CharSet = CharSet.Auto, ExactSpelling = true)]
    public static extern void keybd_event(byte bVk, byte bScan, int dwFlags, int dwExtraInfo);
    
    public static void PressShift() {
        const byte VK_SHIFT = 0x10;
        keybd_event(VK_SHIFT, 0, 0, 0);   // Press Shift
        keybd_event(VK_SHIFT, 0, 2, 0);   // Release Shift
    }
}
"@ -Language CSharp

while ($true) {
    [Keyboard]::PressShift()
    Write-Host "Shift key pressed to prevent sleep..." -ForegroundColor Green
    Start-Sleep -Seconds 60  # Adjust time as needed
}

```
---

`rundll32.exe powrprof.dll,SetSuspendState Sleep`

---

```python

from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType, IntegerType
from itertools import chain

# Define the modified UDF to create MapType columns from multiple attributes, using only integer suffixes as keys
def convert_integer_suffixed_column_to_map(df, value_col_name: str) -> "DataFrame":
    # Create a list of key-value pairs where the key is the integer suffix and value is the column value
    return df.withColumn(
        "credit_interest_band_limit_type",  # New column with the merged MapType
        F.create_map(
            *list(
                chain(
                    *[
                        (
                            F.lit(value_col.split("_")[-1])  # Extract the integer part of the column name
                            .cast(StringType()),  # Column suffix (integer) as the string key
                            F.col(value_col),  # Corresponding column value
                        )
                        for value_col in df.columns
                        if value_col_name in value_col  # Filter columns with the specified suffix pattern
                    ]
                )
            )
        ).cast(MapType(StringType(), IntegerType()))  # Create MapType column with String keys (integers as strings) and Integer values
    )

# Sample DataFrame
data = [
    (1000, 2000, 3000, 4000),  # Values corresponding to the columns
]

columns = ['tier_1_amt', 'tier_2_amt', 'tier_3_amt', 'tier_4_amt']

df_dev1 = spark.createDataFrame(data, columns)

# Apply UDF to create 'credit_interest_band_limit_type' column as a map
df_dev1 = convert_integer_suffixed_column_to_map(df_dev1, "tier")

# Show the result
df_dev1.show(truncate=False)





```
