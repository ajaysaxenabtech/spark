

---
```python
Thanks for letting me know! Here's the **complete list of test cases** in plain text format so you can easily copy, store, or convert it into your own format:

---

### ✅ **Test Cases for `final_file_pblu` Validation**

---

#### **TC1: Validate schema of `final_file_pblu`**
- **Description**: Ensure all required columns are present.
- **Validation Steps**:
  - Print `final_file_pblu.columns`.
  - Confirm it includes at least:
    - `'subject_to_physical_risk'`
    - `'o_over_s_amount'`
    - `'riskno'`
    - `'acute_risks'`
    - `'chronic_risks'`
    - `'could_be_mapped'`, etc.

---

#### **TC2: Validate row count after deduplication**
- **Description**: Ensure deduplication logic reduced or preserved record count appropriately.
- **Validation Steps**:
  - Compare `final_file_pblu.count()` ≤ `unsecured_file4.count()` and `pblu2.count()`.

---

#### **TC3: Validate `row_number` partitioning logic**
- **Description**: Each group should have only one selected record.
- **Validation Steps**:
  - Check for duplicates in business key used in `Window.partitionBy(...)`.

---

#### **TC4: Validate `riskno` assignment**
- **Description**: Confirm correct mapping of source type.
- **Validation Steps**:
  - Count of distinct values in `riskno` must be subset of `{1, 2, 3}`.
  - Cross-check with values in `final_source`.

---

#### **TC5: Validate `der_risk` logic**
- **Description**: Flag should be 1 if `high_acute_risk` or `high_chronic_risk` is 1.
- **Validation Steps**:
  - Filter where `der_risk == 1` and confirm at least one of those two columns is also 1.

---

#### **TC6: Validate `subject_to_physical_risk` logic**
- **Description**: Should be "Yes" if any high risk present.
- **Validation Steps**:
  - `subject_to_physical_risk = "Yes"` only when at least one of the three risk flags is 1.

---

#### **TC7: Validate coalesced hazard columns**
- **Description**: Ensure final hazard values are correctly coalesced.
- **Validation Steps**:
  - Columns like `river_flood`, `urban_flood`, etc. should not be null when corresponding iteration columns had data.

---

#### **TC8: Validate `could_be_mapped` logic**
- **Description**: Should be "Yes" if both acute and chronic risk columns are not null.
- **Validation Steps**:
  - Filter where `acute_risks` and `chronic_risks` are not null → expect `could_be_mapped == "Yes"`.

---

#### **TC9: Validate `o_over_s_amount` aggregation**
- **Description**: Ensure group-wise sums are logically consistent.
- **Validation Steps**:
  - `sum(o_over_s_amount)` by group (`subject_to_physical_risk`, `could_be_mapped`) must match overall sum.

---

#### **TC10: Validate filtering logic for "No" risk**
- **Description**: Confirm filtering of "No" physical risk works as expected.
- **Validation Steps**:
  - Count distinct `ctpt_id` where `subject_to_physical_risk == 'No'` should match manually filtered data.

---

If you need **PySpark validation code snippets** for any of these test cases, just say the test case number (e.g., “TC4 code”). Would you like to start with that?
```


---
