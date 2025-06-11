Thank you Ajay. Based on the email instruction and the analysis you've already done, here is a **precise, professional step-by-step plan** tailored to your task:

---

## 📝 **Your Task (from Email)**

> Generate the MGID’s from GCDU for **3 consecutive months** to help understand the gap and report it back to the central team.

---

## ✅ Objective

Compare **CIN → MGID** mappings across:

* April (20250430),
* May (20250531),
* June (20250606)

And identify:

* CINs **present in all 3 months**
* MGID mismatches (i.e., MGID changed or dropped in any month)
* Cases where MGID was **missing in April/May but present in June**

---

## 🛠️ Implementation Plan (Structured)

### 🔹 Step 1: Read GCDU Files for All 3 Months

```python
gcdu_apr = spark.read.parquet("hdfs:///apps/hive/warehouse/esg/internal/wholesale/monthly/20250430/gcdu_cda.parquet")
gcdu_may = spark.read.parquet("hdfs:///apps/hive/warehouse/esg/internal/wholesale/monthly/20250531/gcdu_cda.parquet")
gcdu_jun = spark.read.parquet("hdfs:///apps/hive/warehouse/esg/internal/wholesale/monthly/20250606/gcdu_cda.parquet")
```

---

### 🔹 Step 2: Filter Only Relevant Rows for MGID Mapping

(`group_system_id == 'HGHQ'` and `mastergroup_id is not null`)

```python
def extract_mgid(df, month):
    return df.where("group_system_id = 'HGHQ' AND mastergroup_id is not null") \
             .select("cin", "gid", "mastergroup_id") \
             .withColumnRenamed("mastergroup_id", f"mgid_{month}")

df_apr = extract_mgid(gcdu_apr, "apr")
df_may = extract_mgid(gcdu_may, "may")
df_jun = extract_mgid(gcdu_jun, "jun")
```

---

### 🔹 Step 3: Join the Datasets on `cin`

To get a complete picture of CIN → MGID mapping across months:

```python
from functools import reduce

dfs = [df_apr, df_may, df_jun]
df_all = reduce(lambda l, r: l.join(r, on="cin", how="outer"), dfs)

df_all.show(truncate=False)
```

---

### 🔹 Step 4: Filter Out CINs with Inconsistencies

```python
df_issues = df_all.filter("mgid_apr != mgid_may OR mgid_may != mgid_jun OR mgid_apr != mgid_jun")
```

---

### 🔹 Step 5: Export to Pandas and Excel (Optional)

```python
df_issues.toPandas().to_excel("MGID_Mapping_Discrepancy_Report.xlsx", index=False)
```

---

## 📌 Deliverable to Send

Send the central team:

* A brief note: the root cause is `HGHQ` data absence in April and May.
* The Excel comparison showing MGID drop/change across the 3 months.
* Mention that **logic is correct**, and issue is due to missing source data.

---

## 📩 Sample Summary for Email

---

Hi Vikram,

As per the task, I extracted MGID mappings from GCDU for April (20250430), May (20250531), and June (20250606), filtering only records where `group_system_id = 'HGHQ'` and `mastergroup_id is not null`.

📌 **Key Finding**:

* `HGHQ` records were **missing in April and May**, which caused the MGIDs to be dropped for those months.
* In June, `HGHQ` is present again, and MGIDs reappear for the same CINs.

I’ve attached a comparison file listing CINs with differing MGIDs or missing values across these months.

Please let me know if you’d like me to run this check for more months or prepare fallback handling logic.

Regards,
Ajay

---

Let me know if you want me to help generate the Excel directly or modify this into a notebook script.
