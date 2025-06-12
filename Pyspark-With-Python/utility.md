

```


logic_df = input_dict_cls_datamart["logic_df"]

def show_empty_slices(df_logic, cda, check_type):
    empty_attrs = []
    for attr in df_logic["data_attribute_name"].unique():
        slice_ = df_logic[
            (df_logic["data_table_cda"] == cda) &
            (df_logic["check_type"]     == check_type) &
            (df_logic["data_attribute_name"] == attr)
        ]
        if slice_.empty:
            empty_attrs.append(attr)
    print(f"⚠️  Attributes with *no* metadata rows: {empty_attrs}")

show_empty_slices(
    logic_df,
    "eu_taxonomy_germany_datamart_fcd_cda",
    "Post-Check"
)
