
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
    Start-Sleep -Seconds 290  # Adjust time as needed
}

```
---

`rundll32.exe powrprof.dll,SetSuspendState Sleep`

---

```python

from pyspark.sql.functions import col, lit, create_map
from itertools import chain
from pyspark.sql.types import IntegerType

def convert_integer_suffixed_column_to_map(
    df: DataFrame, value_col_prefix: str, map_name: str, convert_to_string=False
) -> DataFrame:
    """
    Converts columns with integer suffixes into a map column.
    
    :param df: Input DataFrame
    :param value_col_prefix: Column prefix to filter relevant columns
    :param map_name: Name for the output map column
    :param convert_to_string: If True, converts double -> integer -> string
    :return: Transformed DataFrame with a new map column
    """
    return df.select(
        *[
            non_value_col
            for non_value_col in df.columns
            if not non_value_col.startswith(value_col_prefix)
        ],
        create_map(
            list(
                chain(
                    *(
                        (
                            lit(value_col.split("_")[-2]).cast(IntegerType()),  # Extract integer part
                            col(value_col).cast(IntegerType()).cast("string") if convert_to_string else col(value_col)  
                        )
                        for value_col in df.columns
                        if value_col.startswith(value_col_prefix)
                    )
                )
            )
        ).alias(map_name),
    )

# Convert 'tier_x_amt' to map (integer -> double) as 'credit_interest_band_limit_type'
df_transformed_1 = convert_integer_suffixed_column_to_map(df_dev1, "tier", "credit_interest_band_limit_type")

# Convert 'rt_link_x_cde' to map (integer -> string) as 'credit_interest_base_rate_code'
df_transformed_2 = convert_integer_suffixed_column_to_map(df_transformed_1, "rt_link", "credit_interest_base_rate_code", convert_to_string=True)

# Show schema after transformation
df_transformed_2.printSchema()

# Show transformed dataframe
df_transformed_2.show(truncate=False)

 


```

---



For your **1-minute introduction** at the **DA_Connect meeting**, here’s a structured and engaging way to present yourself confidently:  

---

### **1-Minute Introduction**  

**1. Start with a Friendly Greeting**  
*"Hi everyone, I’m excited to be part of this forum and to introduce myself today!"*  

**2. Who You Are & Your Background**  
*"My name is [Your Name], and I recently joined DAO ESG as a Data Analyst. I have experience in data analytics, financial risk modeling, and ESG data processing. Before this, I worked on building data pipelines and financial models for HSBC’s risk teams."*  

**3. Your Current Role & Contribution to ESG**  
*"Now, in ESG, I am looking forward to working on data-driven sustainability insights, regulatory reporting, and enhancing ESG analytics for investment and risk assessment."*  

**4. Personal Interests**  
*"Outside of work, I enjoy following financial markets, exploring AI trends, and staying active with jogging. I'm looking forward to collaborating and learning from everyone here!"*  

**5. End with Enthusiasm**  
*"Thanks for having me, and I’m excited to be part of this journey with all of you!"*  

---

💡 **Tips for Delivery:**  
✅ **Keep it conversational and confident** (smile and speak naturally).  
✅ **Pause briefly after each section** (don’t rush through).  
✅ **Make eye contact** (or look into the camera if virtual).  

Would you like me to refine it further or add any specific details? 🚀

---
