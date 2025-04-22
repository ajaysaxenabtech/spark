
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

**Observation Summary for BI Team Presentation**  

### **Objective:**  
Assess the material impact of using the stock establishment code (CODEPOSTALETABLISSEMENT) in DWF evaluations by comparing data consistency, coverage, and financial significance.  

---

### **Key Observations:**  

#### **1. Data Inconsistency in Current Implementation**  
- **Original Analysis (Vikram Sahu’s Comment):**  
  - The summarized numbers were flagged as inconsistent.  
  - Only **10 rows** were processed, with a total exposure of **€3,945**—indicating potential filtering issues or incomplete data.  
  - Fields like `alpaptc_optc` were blank, and others used fallback values (e.g., `postalCode_usage` = "PostalCode").  

- **Updated Analysis (Latest Data):**  
  - **57,973 rows** processed, with total exposure of **€131.05 billion**—showing full coverage when all fields are set to "(All)".  
  - **Discrepancy:** The original filtered approach (using CODEPOSTALETABLISSEMENT) captures negligible exposure vs. the unfiltered dataset.  

#### **2. Material Impact Assessment**  
- **Without Stock Establishment Code (Current State):**  
  - **Coverage:** 100% (all rows included).  
  - **Financial Impact:** €131.05 billion (full portfolio).  
- **With Stock Establishment Code (Proposed State):**  
  - **Coverage:** Near-zero (only 10 rows).  
  - **Financial Impact:** €3,945 (immaterial).  

#### **3. Root Cause Hypothesis**  
- The stock establishment code (**CODEPOSTALETABLISSEMENT**) may:  
  - Be **obsolete** (no longer populated in current data).  
  - Have **mapping issues** (e.g., mismatched field names like `aipaptc_optc` vs. `alpaptc_optc`).  
  - Lack **refresh mechanisms** (static source vs. dynamic data needs).  

---

### **Recommendations:**  
1. **Deprecate Current Source:**  
   - The stock establishment code adds no material value (0.003% coverage of total exposure).  
   - Business team alignment required to confirm.  

2. **Collaborate with External Data Team:**  
   - Identify a **refreshable data source** for postal codes (if still needed).  
   - Validate field mappings (e.g., `postalCode_usage` vs. `CODEPOSTALETABLISSEMENT`).  

3. **Next Steps:**  
   - **BI Team Action:** Present findings to Livia/Reporting team for decision.  
   - **Business Team Discussion:** Confirm deprecation or define new requirements.  

---

### **Visual Proposal for PPT:**  
**Slide 1 (Summary):**  
- **Title:** "DWF Postal Code Evaluation: Stock Establishment Code Impact"  
- **Key Metrics:** Side-by-side comparison of row counts (10 vs. 57,973) and exposure (€3,945 vs. €131B).  
- **Conclusion:** "Current implementation is immaterial; recommend deprecation."  

**Slide 2 (Next Steps):**  
- Flowchart: "Business Approval → Deprecate or Onboard New Source."  

--- 

**Pin:** Reiterate stakeholder alignment (per ticket: *"Coordinate with Business/External Data Teams"*).
---
