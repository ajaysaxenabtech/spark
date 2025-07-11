The run is now crashing **earlier** than the guard you just added:

```text
df = self.df2.withColumn(f"{attribute_name}_test", F.when(logic, 1).otherwise(0))
                                            ▲ here: `logic` is still a *string*
```

That line executes **before** the new “missing-column” check, so Spark tries to
parse the expression in `logic` and immediately dies when it sees the absent
`grca_entity_code`.

---

## Fix 1 – move the guard \_above\_ every Spark call

```python
def functional_check(self, attribute_name, logic):
    # 0️⃣  choose the DataFrame the rule uses
    df_cols = set(self.df2.columns)      # or df1 | df2, if needed

    # 1️⃣  guard BEFORE any F.when / F.expr
    runnable, missing = _logic_runnable(logic, df_cols)
    if not runnable:
        logging.warning(
            "Skipping rule %s for %s (%s): missing %s",
            attribute_name, self.cda, self.check_type, ", ".join(sorted(missing))
        )
        result_dict   = {}
        validate_dict = {
            "test_result": {"test": False},
            "Pass": [],
            "Fail": [],
            "Exception": list(missing),
        }
        return result_dict, validate_dict        # <-- EARLY EXIT

    # 2️⃣  only reach Spark if it's safe
    df = self.df2.withColumn(
        f"{attribute_name}_test", F.when(F.expr(logic), 1).otherwise(0)
    )
    # … rest of the original method …
```

*Key idea*: **guard first, Spark second**.

---

## Fix 2 – make the helper tolerant of non-string cells

Those “expected string or bytes-like object” messages appear when the
`logic` column holds a `None`/`float`.  Patch the regex helper once:

```python
def _columns_used(expr) -> set:
    if not isinstance(expr, str):
        return set()
    return set(_COL_RE.findall(expr))
```

Now `_logic_runnable()` won’t choke on non-string rows.

---

## Quick checklist

| Step | Do this                                                                                        | Why                                                       |
| ---- | ---------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| 1    | Move the guard block **above** every `F.when`, `F.expr`, or Spark action that touches `logic`. | Prevents Spark from ever seeing an unsafe expression.     |
| 2    | Patch `_columns_used()` to ignore non-strings.                                                 | Eliminates “expected string or bytes-like object” errors. |
| 3    | **Restart the kernel**, rerun Pre-Check.                                                       | Ensures the edited module is re-imported.                 |

After this you should see only log lines like

```
INFO  controls_fcd: Skipping ausgabe_datum rule – missing grca_entity_code
```

…and the pipeline will continue to the Excel / DB output.

Ping me with the new log if anything else pops up—we’re very close!
