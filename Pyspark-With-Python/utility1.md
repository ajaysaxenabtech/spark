### What we just proved

* The six attributes you still want to test **do have valid logic strings** (your screenshot confirms no `grca_entity_code` inside them).
* Yet Spark still crashes **inside those rules** with
  `cannot resolve \`grca\_entity\_code\`\`.
* That means the failing code path is **not inside the logic string itself** but somewhere later in `functional_check` (or a helper it calls) that **always references `grca_entity_code` when it builds `test_result_df` / `exception_result_df`.**

In other words, even if the rule’s expression is clean, the *post-processing step* that builds the result frames tries to keep the `grca_entity_code` column for context—and that column does not exist in your Pre-Check DataFrame.

---

## Two ways to unblock quickly

| Fix                                                            | Code                                                                                                                                                                                                                                                                                             | Effect                                                                         |
| -------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------ |
| **A (add a dummy nullable column—fastest, safe)**              | `python\n# run once, just before any Pre-Check validations:\nif self.check_type.lower().startswith(\"pre\"):\n    if \"grca_entity_code\" not in self.df2.columns:\n        from pyspark.sql import functions as F\n        self.df2 = self.df2.withColumn(\"grca_entity_code\", F.lit(None))\n` | All six rules now run; `grca_entity_code` is simply NULL in Pre-Check outputs. |
| **B (re-write the helper that adds context columns—cleanest)** | Remove every unconditional `column_select.insert( … 'grca_entity_code' … )` or `df.select('grca_entity_code', …)` call **unless** that column is present.                                                                                                                                        | Keeps the DF schema minimal but requires a code search/edit.                   |

The dummy-column (A) takes 3 lines and fixes today’s block; you can roll
option B later when you refactor.

---

## Where to add fix A

In the same class that owns `df2` (often the constructor or right before the
Pre-Check loop):

```python
def validate_pre_checks(self, spark):
    # --- add the patch here ---
    if self.check_type.lower() == "pre-check":
        if "grca_entity_code" not in self.df2.columns:
            from pyspark.sql import functions as F
            self.df2 = self.df2.withColumn("grca_entity_code", F.lit(None))
    # --- original code continues ---
```

Restart the kernel, rerun Pre-Check:

```
INFO  Skipping grca_entity_code rule…        # still skipped (good)
INFO  Running ausgabe_datum…                 # now executes
INFO  Running icp_id…                        # executes
...
```

And your exception report should show results for the six attributes.

---

### Why this works

* Spark no longer complains because the column now exists (even though it’s NULL).
* Your validation logic (equality checks, `.isNull()`, etc.) still gives correct “Pass/Fail” outcomes—the column being NULL in Pre-Check usually makes those rules **fail** when they should (or you can adapt the rule to treat Pre-Check differently).

---

## Next steps if you want a cleaner long-term fix

1. Grep the repo for `'grca_entity_code'` inside utilities that build
   `column_select`.  Add an `if "grca_entity_code" in df.columns:` guard.
2. Remove the dummy-column patch.

But for now, patch A gets your pipeline running again and validates the six
attributes as intended.
