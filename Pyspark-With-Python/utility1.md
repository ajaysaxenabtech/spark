

---

| Task                 | Solution Example                                                    |
| -------------------- | ------------------------------------------------------------------- |
| Force integer string | `format_string("%.0f", col("colname").cast("double"))`              |
| Force decimal string | `format_string("%.9f", col("colname").cast("double"))`              |
| Keep as-is           | `.cast("string")` (can still get scientific notation in some tools) |



```