Below is a **surgical, copy-pasteable recipe** that will protect *every* rule in your engine—once you add it, any logic string that references a column missing from the DataFrame will be **skipped gracefully + logged**, instead of taking down the whole run.

> **Files you’ll touch**: the main module that already holds `functional_check`, `validate_pre_checks`, and the attribute-specific helpers (e.g. `controls_fcd.py`).
> **Assumptions**: rules always evaluate against `self.df2` (if a few special rules need `df1`, you can pass that column list instead—details below).

---

## 1  Add two tiny helpers (one time only)

Put these **near the top of the file, after the normal imports**:

```python
import re, logging
# ----------------------------------------------------
#  regex: grabs every column name inside F.col('...')
# ----------------------------------------------------
_COL_RE = re.compile(r"""F\.col\(['"]([^'"]+)['"]\)""")

def _columns_used(expr: str) -> set[str]:
    """Return all column names referenced in a PySpark expr string."""
    return set(_COL_RE.findall(expr))

def _logic_runnable(expr: str, df_cols: set[str]) -> tuple[bool, set[str]]:
    """
    Returns (True, set()) if all required columns exist in df_cols.
    Otherwise (False, <missing columns>).
    """
    missing = _columns_used(expr) - df_cols
    return (len(missing) == 0, missing)
```

*No other part of the code needs to know regex details—call `_logic_runnable()` and you’re done.*

---

## 2  Guard the *central* evaluator (`functional_check`)

Find the part that currently looks something like:

```python
def functional_check(self, attribute_name, logic):
    df = self.df2.withColumn(...)            # existing code
    # ...
    test_result_df = df.filter(F.expr(logic)).select(...)   # <-- BOOM happens here
```

Insert the guard **right before the first `F.expr(logic)`**:

```python
def functional_check(self, attribute_name, logic):
    # ----------------------------------------
    # 1) Decide which DF the rule will hit
    #    (here we assume df2; change if needed)
    # ----------------------------------------
    df_cols = set(self.df2.columns)   # or self.df1.columns for special cases

    # ----------------------------------------
    # 2) Guard check
    # ----------------------------------------
    runnable, missing = _logic_runnable(logic, df_cols)
    if not runnable:
        logging.warning(
            "Skipping rule %s for %s (%s): missing columns %s",
            attribute_name, self.cda, self.check_type, ", ".join(sorted(missing))
        )

        # Return a stub so the caller can still log a result
        result_dict = {}
        validate_dict = {
            "test_result": {"test": False},
            "Pass": [],
            "Fail": [],
            "Exception": list(missing),   # you can rename to 'Skip' if you like
        }
        return result_dict, validate_dict   # <-- early exit, no Spark call

    # ----- ORIGINAL LOGIC continues below -----
    df = self.df2.withColumn(...)           # untouched
    # ... your whole block that uses F.expr(logic) ...
```

**Notes**

* Keep the early-exit `return` consistent with whatever the caller expects (`validate_pre_checks` already unpacks `(result_dict, validate_dict)` so we conform to that).
* If your engine doesn’t need `result_dict` when a rule is skipped, returning `{}` is fine.

---

## 3  (Optionally) patch any *attribute-specific* helpers

If you have helper functions that call `F.expr()` *directly* (not via `functional_check`), add the same pattern there.
Example:

```python
def ausgabe_datum_logic(self, df_logic):
    mask = ...
    logic_str = df_logic.loc[mask, "logic"].iat[0]

    runnable, missing = _logic_runnable(logic_str, set(self.df2.columns))
    if not runnable:
        logging.info("Skip ausgabe_datum logic – missing %s", missing)
        return df_logic                       # harmless fallback

    df_logic.loc[mask, "logic"] = logic_str
    return df_logic
```

---

## 4  Restart kernel → run Pre-Check again

**Expected console output**

```
INFO  controls_fcd: Skip ausgabe_datum rule for eu_taxonomy_germany_datamart_fcd_cda (Pre-Check) – missing columns grca_entity_code
INFO  controls_fcd: Skip icp_id rule for eu_taxonomy_germany_datamart_fcd_cda (Pre-Check) – missing columns grca_entity_code
... (etc.)
```

*No* Spark stack-trace should appear, and the rest of the pipeline proceeds.

---

## 5  If a rule really needs both df1 and df2

Change the guard line accordingly:

```python
df_cols = set(self.df1.columns) | set(self.df2.columns)
```

or pass whichever DF you plan to use inside `F.expr`.

---

### That’s it!

* One guard, one place → **all future “cannot resolve” errors collapse into a clean “Skip” log line**.
* You can still tidy the CSV later to reduce skipped rules, but you’ll never break the run again.

Ping me with the log output after you implement steps 1–4, and we can tweak the `validate_dict` structure or the log wording if you need something different for downstream reporting.
