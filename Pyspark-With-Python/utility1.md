1012   def functional_check(self, attribute_name, logic):
+1013       # ── UNIVERSAL GUARD: skip rule if ANY referenced column is missing ──
+1014       df2_cols = set(self.df2.columns)
+1015       runnable, missing = _logic_runnable(logic, df2_cols)
+1016       if not runnable:
+1017           logging.warning(
+1018               "Skipping rule %s for %s (%s): missing %s",
+1019               attribute_name, self.cda, self.check_type,
+1020               ", ".join(sorted(missing))
+1021           )
+1022           # stub out the expected return shape
+1023           result_dict = {}
+1024           validate_dict = {
+1025               "test_result": {"test": False},
+1026               "Pass": [],
+1027               "Fail": [],
+1028               "Exception": list(missing),
+1029           }
+1030           return result_dict, validate_dict
+1031       # ── end guard ──

1032       # now it’s safe to reference `logic` in any Spark call…
1033       string_dtype_columns = self.string_type_columns(self.df2)
1034       output_df_columns   = self.df2.columns
1035       # …

