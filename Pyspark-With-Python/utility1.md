### Where exactly to drop the guard

Look for the **first place inside `functional_check`** where the logic string is given to Spark—usually a call to `F.expr(logic)` or `df.filter(F.expr(logic))`.
That is the **spot you must protect**.

If your file shows something like (line numbers are only examples):

```python
1011     def functional_check(self, attribute_name, logic):
1012         # … earlier prep work …
1013 
1014         # build a working DataFrame
1015         df = self.df2.withColumn(f"{attribute_name}_test", F.when(logic, 1).otherwise(0))
1016 
1017         test_result_df = df.filter(F.expr(logic))        # ← Spark touch-point
1018         column_select = self.unique_column_select()
1019         # rest of the method …
```

then **insert the guard immediately *before* line 1017**, i.e. between 1016 and 1017 (your editor might show it as 1016–1017 or 1017–1019 depending on blank lines).

```python
1015     df = self.df2.withColumn(f"{attribute_name}_test", F.when(logic, 1).otherwise(0))

# ----------------------------------------------------------------
# UNIVERSAL GUARD – paste the block shown earlier right here
# ----------------------------------------------------------------
        df_cols = set(self.df2.columns)      # or df1/union, as needed
        runnable, missing = _logic_runnable(logic, df_cols)
        if not runnable:
            logging.warning(
                "Skipping rule %s for %s (%s): missing %s",
                attribute_name, self.cda, self.check_type, ", ".join(sorted(missing))
            )
            result_dict = {}
            validate_dict = {
                "test_result": {"test": False},
                "Pass": [],
                "Fail": [],
                "Exception": list(missing),
            }
            return result_dict, validate_dict   # ← early exit
# ----------------------------------------------------------------

1017     test_result_df = df.filter(F.expr(logic))
```

**Checklist**

1. The guard block sits *before* any `F.expr(logic)` (or `.when(logic, …)` if `logic` is a string rather than a boolean column).
2. It returns early if columns are missing; otherwise the original code runs unmodified.
3. Add the two helper functions (`_columns_used`, `_logic_runnable`) near the top of the file only once.

After you paste the guard, restart the kernel and run the Pre-Check again—you should now see “Skipping rule … missing columns …” in the log instead of Spark’s “cannot resolve” error.

Let me know how it goes!
