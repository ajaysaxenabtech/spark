In your screenshot the very *first* place you hand Spark your `logic` string is here:

```python
1019     df = self.df2.withColumn(  
1020         f"{attribute_name}",  
1021         F.trim(F.col(f"{attribute_name}"))  
1022     )  
1023  
1024     df = df.withColumn(  
1025         f"{attribute_name}_test",  
1026         F.when(logic, 1).otherwise(0)           # ← **this** uses `logic`  
1027     )  
```

You must put the guard **before** line 1026 so that you never call `F.when(logic, …)` unless `logic` is safe. For example:

```diff
 1019     df = self.df2.withColumn(
 1020         f"{attribute_name}",
 1021         F.trim(F.col(f"{attribute_name}"))
 1022     )
 1023
+1024     # --- UNIVERSAL GUARD: bail out if any column in `logic` is missing ---
+1025     df_cols = set(self.df2.columns)
+1026     runnable, missing = _logic_runnable(logic, df_cols)
+1027     if not runnable:
+1028         logging.warning(
+             "Skipping rule %s for %s (%s): missing %s",
+             attribute_name, self.cda, self.check_type,
+             ", ".join(sorted(missing))
+         )
+1029         result_dict   = {}
+1030         validate_dict = {
+1031             "test_result": {"test": False},
+1032             "Pass": [],
+1033             "Fail": [],
+1034             "Exception": list(missing),
+1035         }
+1036         return result_dict, validate_dict   # ← early exit
+1037     # --- end guard ---
 1038
 1039     df = df.withColumn(
 1040         f"{attribute_name}_test",
 1041         F.when(logic, 1).otherwise(0)      # ← now safe, because we’ve gated it
 1042     )
```

**Key points:**

1. **Line 1026** (`F.when(logic, …)`) is the very first Spark call that uses your `logic` string—so the guard must be immediately before it.
2. Make sure you `return` out of `functional_check` in that guard, so none of the downstream Spark expressions ever see a bad `logic`.
3. After you’ve done that, you can leave your later call at line 1041 (`df.filter(F.expr(logic))`) untouched, because by then you know it’s safe.

Once you insert it at **1024–1037**, restart your kernel and rerun Pre-Check: you should get only “Skipping rule …” warnings and no more “cannot resolve” crashes.
