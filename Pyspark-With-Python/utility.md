Below is a **surgical way to drop-in the diagnostic code** so you can see *exactly* which internal filter is producing an empty slice. After that you’ll know which part of your metadata (or which helper) must be fixed.

---

## 1  Add a tiny helper inside `EUTaxonomyFCDTestControls`

Put this right at the top of the class (just under the `class …:` line, before `__init__`):

```python
# ────────────────────────────────────
# Small helper – prints slice size
# ────────────────────────────────────
def _debug_slice(self, df, label):
    rows = len(df) if df is not None else 0
    print(f"[DEBUG] {label}: {rows} rows")
    if rows == 0:
        import traceback, sys
        traceback.print_stack(file=sys.stdout)
```

---

## 2  Drop debug calls at every narrowing step

In your screenshot, the first time `self.logic_df` is assigned is here:

```python
self.logic_df = self.grca_logic(kwargs.get("logic_df"))
self.logic_df = self.grca_entity_code_logic(self.logic_df)
```

Add a `_debug_slice` **after each assignment**.
Example:

```python
# 1. after grca_logic
self.logic_df = self.grca_logic(kwargs.get("logic_df"))
self._debug_slice(self.logic_df, "after grca_logic")

# 2. after grca_entity_code_logic
self.logic_df = self.grca_entity_code_logic(self.logic_df)
self._debug_slice(self.logic_df, "after grca_entity_code_logic")
```

Do the same for any other helper that returns a filtered `DataFrame` (e.g. `condition_filter`, `get_all_completeness_logic`, etc.):

```python
logic_df_cond = self.condition_filter(self.df_logic_all,
                                      self.table_name,
                                      self.check_type)
self._debug_slice(logic_df_cond, "after condition_filter")
```

---

## 3  Run the pipeline

You will see a series of lines like:

```
[DEBUG] after grca_logic: 123 rows
[DEBUG] after grca_entity_code_logic: 0 rows   ← this is the one that empties out
```

When a slice prints **0 rows**, the stack-trace that follows (we captured it with `traceback.print_stack`) will point to the helper that caused it.

---

## 4  Fix the metadata for that helper

| Helper that empties slice    | Extra column it relies on                                               | What to check in `reference_logics_all.csv`                                         |
| ---------------------------- | ----------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `grca_logic`                 | `internal_category == 'GRCA'`                                           | No `NaN` in **internal\_category**; must equal `GRCA` or whatever the helper tests. |
| `grca_entity_code_logic`     | Same, plus country-specific list                                        | Same as above *and* the logic expression isn’t empty.                               |
| `condition_filter`           | Requires `check_type` & `data_table_cda`                                | Make sure those columns exactly match.                                              |
| `get_all_completeness_logic` | expects `row_count`, `balance_sheet_sum`, etc. in `data_attribute_name` | Add rows for those attributes if you use completeness checks.                       |

---

## 5  (Optional) Replace `.iloc[0]` with a safe accessor

Wherever the code currently has:

```python
val = df_slice["data_attribute_name"].iloc[0]
```

patch it:

```python
def first_or_none(series):
    return series.iloc[0] if len(series) else None

val = first_or_none(df_slice["data_attribute_name"])
if val is None:
    raise ValueError(
        f"No metadata rows left for {self.cda}/{self.check_type} "
        f"after {helper_name}. Check reference_logics_all.csv."
    )
```

This turns a cryptic pandas **indexer** error into a readable message.

---

### Summary

1. **Paste `_debug_slice`** helper at the top of the class.
2. **Call it** after every filter step (`grca_logic`, `grca_entity_code_logic`, `condition_filter`, etc.).
3. **Run once** – the first slice that prints “0 rows” is your culprit.
4. **Fix the metadata** (NaNs, spelling, trailing commas) for exactly the field that helper uses.
5. **(Optional) guard** all `.iloc[0]` with the `first_or_none` pattern to prevent this crash forever.

Once that empty slice is gone, the “single positional indexer is out-of-bounds” error will disappear for Germany Datamart just like it did for France.
