Below is a **self-contained rewrite** of your entire `functional_check` method. It

1. **Drops any earlier guard** you might have scattered inside branches
2. **Implements one universal guard** at the top
3. Proceeds to your original logic, untouched, knowing `logic` is now safe

```python
import logging
from pyspark.sql.functions import col, expr, when

# … your existing imports …

# At the top of your file, make sure you have:
import re
_COL_RE = re.compile(r"""F\.col\(['"]([^'"]+)['"]\)""")

def _columns_used(expr):
    if not isinstance(expr, str):
        return set()
    return set(_COL_RE.findall(expr))

def _logic_runnable(expr, df_cols):
    missing = _columns_used(expr) - df_cols
    return (len(missing) == 0, missing)


class YourClass:
    # … other methods …

    def functional_check(self, attribute_name, logic):
        """
        Runs the functional rule (given as a Spark expression string) against df2,
        returning a tuple (result_dict, validate_dict).
        """

        # ── UNIVERSAL GUARD: bail out before any Spark use of `logic` ──

        # 1) Non-string logic is an immediate skip
        if not isinstance(logic, str):
            logging.warning(
                "Skipping %s for %s (%s): logic is not a string",
                attribute_name, self.cda, self.check_type
            )
            return {}, {
                "test_result": {"test": False},
                "Pass": [],
                "Fail": [],
                "Exception": ["<no-logic>"]
            }

        # 2) Check that every column F.col(...) in the logic exists in df2
        df2_cols = set(self.df2.columns)
        runnable, missing = _logic_runnable(logic, df2_cols)
        if not runnable:
            logging.warning(
                "Skipping %s for %s (%s): missing columns %s",
                attribute_name,
                self.cda,
                self.check_type,
                ", ".join(sorted(missing))
            )
            return {}, {
                "test_result": {"test": False},
                "Pass": [],
                "Fail": [],
                "Exception": list(missing)
            }

        # ── At this point we know `logic` is a valid string and all its columns exist ──

        # Your original code starts here
        string_dtype_columns = self.string_type_columns(self.df2)
        output_df_columns   = self.df2.columns

        if attribute_name in string_dtype_columns and attribute_name in output_df_columns:
            df = (
                self.df2
                    .withColumn(attribute_name, F.trim(col(attribute_name)))
            )
        else:
            df = self.df2

        # add the test column
        df = df.withColumn(
            f"{attribute_name}_test",
            when(expr(logic), 1).otherwise(0)
        )

        # now build test_result_df / exception_result_df exactly as before
        test_result_df = df.filter(expr(f"{attribute_name}_test == 1"))
        column_select = self.unique_column_select()
        column_select.insert(2, attribute_name)
        column_select_ordered = sorted(
            list(set(column_select)),
            key=lambda x: column_select.index(x)
        )
        exception_result_df = df.filter(expr(f"{attribute_name}_test == 0")) \
                                .select(*column_select_ordered)

        correct_count   = test_result_df.count()
        incorrect_count = exception_result_df.count()
        row_count       = df.count()

        result_dict = {
            "test_result_df": test_result_df,
            "exception_result_df": exception_result_df,
            "correct_count": correct_count,
            "in-correct_count": incorrect_count,
            "row_count": row_count
        }

        test_passed = (correct_count == row_count) or (incorrect_count == 0)
        validate_dict = {
            "test_result": {"test": test_passed},
            "Pass": self.get_list_of_values(test_result_df, attribute_name),
            "Exception": self.get_list_of_values(exception_result_df, attribute_name),
            "Fail": self.get_list_of_values(exception_result_df, attribute_name)
                    + self.get_list_of_values(test_result_df, attribute_name)
        }

        return result_dict, validate_dict
```

### What you need to do

1. **Delete any earlier guards** you added between line 1024–1037 or inside the string-dtype branch.
2. Copy-paste the **one** guard block shown above (lines `if not isinstance…` through `return …`) immediately after the `def functional_check` signature (before any other code).
3. Ensure your helper functions are defined as shown (*no Python 3.9+ generics*).
4. **Restart** your notebook kernel and rerun the Pre-Check cell.

You should now see **only** `"Skipping … missing columns …"` or `"Skipping … logic is not a string"` warnings, and **no** Spark errors. Let me know how it behaves!
