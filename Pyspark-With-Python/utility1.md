I dug in again and I can now see why you’re still getting those “expected string or bytes-like object” crashes:

1. **Your guard landed inside the first `if attribute_name in string_dtype_columns` branch**, so any attribute that isn’t caught there (e.g. `ausgabe_datum`, `icp_id`, `branche_finrep_adjusted`, etc.) simply skips the guard entirely and hits Spark unprotected.
2. Also, your `_columns_used()` helper still assumes `expr` is a string, so when `logic` is actually `None` or a float, it blows up before your guard even runs.

---

## Two fixes that together seal the hole

### 1) Move the guard to the *very top* of `functional_check`

Right after the signature, before *any* `if` or Spark call:

```diff
1012   def functional_check(self, attribute_name, logic):
+1013       # ── UNIVERSAL GUARD: skip any logic that mentions missing columns or isn’t a string ──
+1014       # 1. handle non-strings
+1015       if not isinstance(logic, str):
+1016           missing = ["<no-logic>"]
+1017           logging.warning("Skipping %s for %s (%s): logic is not a string",
+1018                           attribute_name, self.cda, self.check_type)
+1019           return {}, {"test_result":{"test":False}, "Pass":[], "Fail":[], "Exception": missing}
+
+1020       # 2. check for missing columns
+1021       df2_cols = set(self.df2.columns)
+1022       runnable, missing = _logic_runnable(logic, df2_cols)
+1023       if not runnable:
+1024           logging.warning("Skipping %s for %s (%s): missing %s",
+1025                           attribute_name, self.cda, self.check_type,
+1026                           ", ".join(sorted(missing)))
+1027           return {}, {"test_result":{"test":False}, "Pass":[], "Fail":[], "Exception": missing}
+1028       # ── end universal guard ──
```

That way **no matter which branch** (`string_dtype` or numeric) the attribute takes, **we bail out first**.

---

### 2) Make `_columns_used` safe on non-strings

In the helper near the top of your module, change:

```python
def _columns_used(expr):
    return set(_COL_RE.findall(expr))
```

to

```python
def _columns_used(expr):
    if not isinstance(expr, str):
        return set()
    return set(_COL_RE.findall(expr))
```

This prevents the “expected string or bytes-like object” before your guard can run.

---

### What to do now

1. **Paste the new guard (lines 1013–1028) at the top of `functional_check`.**
2. **Fix `_columns_used`** as shown.
3. **Restart your notebook kernel** to reload the edited module.
4. **Run your Pre-Check cell again.**

You should now see only log‐warnings like:

```
Skipping ausgabe_datum for eu_taxonomy_germany_datamart_fcd_cda (Pre-Check): missing grca_entity_code
```

…and **no more** “expected string” or “cannot resolve” errors. Let me know how it goes!
