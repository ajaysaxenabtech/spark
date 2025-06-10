### What we learned from the quick probe

| Metric                                | Result                                                            | Meaning                                            |
| ------------------------------------- | ----------------------------------------------------------------- | -------------------------------------------------- |
| **Total Post-Check rules**            | **123**                                                           | You **do** have metadata for Germany-Datamart      |
| **Null counts**                       | `data_table_cda = 0`, `check_type = 0`, `data_attribute_name = 0` | The three core columns are present on all 123 rows |
| **Rows in slice (table+check\_type)** | **123**                                                           | The first slice that feeds `.iloc[0]` is not empty |

Yet you still hit **`single positional indexer is out-of-bounds`**.
That means a *later* slice—built inside one of the helper methods—is empty.
From the code you shared earlier, the next filter‐step is something like:

```python
change_cond = (
    (df_logic['data_table_cda'] == self.cda) &
    (df_logic['check_type']     == self.check_type) &
    (df_logic['data_attribute_name'] == '<attr-name - filled later>')
)
df_logic_attr = df_logic[change_cond]
data_attribute_name = df_logic_attr['data_attribute_name'].iloc[0]   # ← explodes if df_logic_attr empty
```

So the empty slice must occur **per attribute**.
Why would that happen if the main slice has 123 rows?  Two common reasons:

1. **Extra blank columns (“Unnamed 6 … Unnamed 34”)**
   Those *trailing delimiters* in `reference_logics_all.csv` shift the real columns sideways for some rows, leaving `data_attribute_name` or `logic` **NaN** for those rows.

2. **Trailing spaces / mismatched spelling** in `data_attribute_name` so the code later looks for an attribute it never finds.

---

## How to clean the metadata once and for all

> **Fix this in the CSV/Excel**; no Python changes are needed once the data is clean.

### 1  Rip out the bogus “Unnamed” columns

Open **`reference_logics_all.csv`** in a text editor and you’ll see commas after the last real column:

```
table_name,check_type,data_table_cda,data_attribute_name,logic,validation_requirements,
eu_taxonomy_germany_datamart_fcd_cda,Post-Check,...,,
```

Delete those extra commas **or** tell pandas to ignore them:

```python
df_logic = (
    pd.read_csv("reference_logics_all.csv", index_col=False)
      .loc[:, ~df_logic.columns.str.contains(r'^Unnamed')]
)
df_logic.to_csv("reference_logics_all.csv", index=False)
```

### 2  Confirm every attribute row is complete

```python
bad_rows = df_logic[
    df_logic["table_name"].eq("eu_taxonomy_germany_datamart_fcd_cda") &
    df_logic["check_type"].eq("Post-Check") &
    (
        df_logic["data_attribute_name"].isna() |
        df_logic["logic"].isna()
    )
]
print(bad_rows)
```

If you get anything > 0, fill in the missing `data_attribute_name` or `logic`, or delete the row.

### 3  Trim stray whitespace

```python
for col in ["table_name", "check_type", "data_attribute_name"]:
    df_logic[col] = df_logic[col].str.strip()
df_logic.to_csv("reference_logics_all.csv", index=False)
```

### 4  (Option) Add a defensive guard

Inside **`ReferenceLogic.condition_filter`** or the first place you use `.iloc[0]`:

```python
if df_logic_attr.empty:
    raise ValueError(
        f"No metadata row for {self.cda} / {self.check_type} / {attr_name}. "
        "Check reference_logics_all.csv."
    )
```

That turns a cryptic pandas error into a meaningful message.

---

## Quick retest checklist

1. **Reload** the notebook / restart the Spark session (so the CSV is re-read).
2. Run up to the point where you instantiate `EUTaxonomyFCDTestControls`.
3. If it still fails, print the very attribute name that produces the empty slice:

```python
for attr in logic_df["data_attribute_name"].unique():
    slice_ = logic_df[
        (logic_df["data_table_cda"] == self.cda) &
        (logic_df["check_type"]     == self.check_type) &
        (logic_df["data_attribute_name"] == attr)
    ]
    if slice_.empty:
        print("⚠️ EMPTY  →", attr)
```

Fill those missing rows in the CSV and re-run—problem solved.

---

### Why this happens only for some CDAs

* France rows had no trailing commas, so their `logic` and `data_attribute_name` columns loaded fine.
* Malta and Germany rows were shifted by the stray commas, producing NaNs in critical columns—hence `.iloc[0]` fails there.

Clean the CSV once, and **all** CDAs will initialise cleanly.
