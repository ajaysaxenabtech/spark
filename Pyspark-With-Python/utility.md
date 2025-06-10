

```
# ------------------------------------------------------------------
# 1️⃣  How many rules SURVIVED the Germany filter?
# ------------------------------------------------------------------
logic_df = input_dict_cls_datamart["logic_df"]
print("Total rules for Germany datamart (Post-Check):", len(logic_df))

# ------------------------------------------------------------------
# 2️⃣  Are any of the key columns null?
# ------------------------------------------------------------------
null_stats = (
    logic_df[["data_table_cda", "check_type", "data_attribute_name"]]
    .isna()
    .sum()
)
print("Null counts ⤵️")
print(null_stats)

# ------------------------------------------------------------------
# 3️⃣  Show me the rows that WILL feed the first .iloc[0] call
#     (the code usually filters on check_type and data_table_cda first)
# ------------------------------------------------------------------
first_slice = logic_df.query(
    "data_table_cda == 'eu_taxonomy_germany_datamart_fcd_cda' and check_type == 'Post-Check'"
)
print("Rows in the slice that triggers .iloc[0]:", len(first_slice))
print(first_slice.head())
