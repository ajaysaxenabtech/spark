
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

# Observation Summary Explanation

An observation summary in this context would be a concise report highlighting key findings, inconsistencies, and notable patterns from the data analysis of the EBA Pillar-3 postal code establishment information.

## Potential Observations from This Data:

1. **Data Completeness Issue**: 
   - The "alpaptc_optc" field shows blank values (✗), indicating missing data that should be investigated

2. **Field Mapping Inconsistencies**: 
   - Multiple fields (CODEPOSTALETABLISSEMENT, CPS_NFCP) are marked with ✗, suggesting potential mapping or validation issues between source and target fields

3. **Limited Data Volume**: 
   - The total row count is only 10, which seems very small for financial reporting - worth verifying if this is the complete dataset

4. **Risk Exposure**: 
   - All records show "Yes" for subject_to_physical_risk, which might be significant for risk reporting

5. **Value Concentration**: 
   - The sum of s_montant_euro_signe_gross is relatively small (3,945), potentially indicating this is either a sample or a very specific subset

## Suggested PPT Structure:

1. **Introduction**: Purpose of analysis
2. **Data Overview**: Sources, time period, scope
3. **Key Observations**: The points above
4. **Data Quality Issues**: Missing/inconsistent fields
5. **Next Steps**: Validation needed, actions required

The summary should help Livia/Reporting team quickly understand the current state of the data and what requires attention before final reporting.

---
