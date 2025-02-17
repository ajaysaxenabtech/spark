
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
from pyspark.sql.functions import col, lit, create_map
from pyspark.sql.types import IntegerType
from pyspark.sql import DataFrame

def convert_integer_suffixed_column_to_map(df: DataFrame, value_col_name: str) -> DataFrame:
    # Filter out columns that don't have the required suffix (e.g., 'tier_X_amt')
    non_value_col = [col_name for col_name in df.columns if value_col_name not in col_name]

    # Applying the UDF logic to convert and map the columns
    return df.select(
        *[
            col(non_value_col),  # Keeping the non-value columns as is
            create_map(
                *[
                    lit(col_name.split("_")[-2]).cast(IntegerType()),  # Extracting the number before '_amt' (e.g., '1' from 'tier_1_amt')
                    col(col_name)
                ]
            ).alias(f"{value_col_name}_{col_name.split('_')[-2]}_mapped")  # Corrected alias
            for col_name in df.columns
            if value_col_name in col_name
        ]
    )

# Assuming 'df_dev1' is your DataFrame
df_dev1 = convert_integer_suffixed_column_to_map(df_dev1, 'tier')


```
