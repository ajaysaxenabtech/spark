
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
from pyspark.sql import DataFrame

def column_completeness(df: DataFrame, column_list: list) -> DataFrame:
    # Create a list of expressions for each column that calculate its non-null completeness
    df_completeness = df.select(
        *[
            (F.format_number(
                F.count(F.when(F.col(c).isNotNull(), c)) * 100 / F.count(F.lit(1)), 2
            ).alias(c))  # Calculate the percentage of non-null values for each column
            for c in column_list  # Iterate over each column in column_list
        ]
    )
    return df_completeness

# Usage example with df_dev1:
column_list = df_dev1.columns  # Assuming df_dev1 is the final DataFrame
df_completeness = column_completeness(df_dev1, column_list)
df_completeness.show()
 


```
