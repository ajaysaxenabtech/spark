
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

from pyspark.sql.functions import col, lit, create_map, when
from itertools import chain
from pyspark.sql.types import IntegerType

def convert_bpt_fac_to_map(df: DataFrame, prefix: str, ind_prefix: str, map_name: str) -> DataFrame:
    """
    Convert bpt_fac_x_rt columns to +ve or -ve based on plus_ms_x_ind values, and create a map.
    
    :param df: Input DataFrame
    :param prefix: Prefix for bpt_fac_x_rt columns
    :param ind_prefix: Prefix for plus_ms_x_ind columns
    :param map_name: Name for the output map column
    :return: Transformed DataFrame
    """
    return df.select(
        *[
            non_value_col
            for non_value_col in df.columns
            if not non_value_col.startswith(prefix)
        ],
        create_map(
            list(
                chain(
                    *(
                        (
                            lit(int(col_name.split('_')[-2])).cast(IntegerType()),  # Extract integer part
                            when(col(ind_col) == 'P', col(col_name))  # Positive when 'P'
                            .when(col(ind_col) == 'N', -col(col_name))  # Negative when 'N'
                            .otherwise(col(col_name))  # Default as-is
                        )
                        for col_name in df.columns
                        if col_name.startswith(prefix)
                        for ind_col in df.columns
                        if ind_col.startswith(ind_prefix) and col_name.split('_')[-2] == ind_col.split('_')[-2]
                    )
                )
            )
        ).alias(map_name)
    )

# Applying the transformation
df_final = convert_bpt_fac_to_map(df_dev1, "bpt_fac", "plus_ms", "credit_interest_spread_factor")

# Display the schema and data
df_final.printSchema()
df_final.show(truncate=False)


```
