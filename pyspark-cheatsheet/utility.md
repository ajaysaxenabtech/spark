

---
```python
| **TC-ID** | **Description** | **Validation Steps** |
|----------|------------------|-----------------------|
| **TC-016** | Validate NULL/blank fields in critical columns | i) Ensure no nulls in critical fields like `subject_to_physical_risk`, `o_over_s_amount`, `final_source` <br> ii) Count records with nulls in these columns |
| **TC-017** | Validate distinct values in categorical columns | i) List unique values in `final_source`, `high_acute`, `high_chronic`, `could_be_mapped` <br> ii) Check they match expected values (`Yes`, `No`, etc.) |
| **TC-018** | Validate record uniqueness by composite key | i) Define a business key (e.g. `ctpt_id`, `nuts_level_3_upper_clean_final`, `final_source`) <br> ii) Ensure no duplicates in `final_file_pblu` |
| **TC-019** | Validate coalescing hierarchy logic | i) For each hazard, check that if iteration_1 is null and iteration_2 is not, the final column picks iteration_2, and so on |
| **TC-020** | Validate exclusion logic for unmapped records | i) Check records where `could_be_mapped == 'No'` <br> ii) Confirm they are not used in aggregate stats or mapping logic |
| **TC-021** | Validate correct source-based riskno logic | i) If `final_source == 'collateral'` then `riskno` must be 1, etc. <br> ii) No cross-assigned riskno values |
| **TC-022** | Validate mapping coverage by geography | i) For all `country_name_upper_clean_final`, calculate mapping rate <br> ii) Share percentage of mapped vs unmapped |
| **TC-023** | Validate row_number deduplication logic | i) Check `row_number == 1` groupings still respect expected grouping constraints |
| **TC-024** | Validate ordering impact on row_number | i) Modify sorting column and check changes in resulting first-row selection |
| **TC-025** | Validate aggregated risk distribution | i) Count how many records fall under each `high_acute`, `high_chronic`, and `both_high` bucket for audit |
```


---

| **TC-ID** | **Description** | **Validation Steps** |
|----------|------------------|-----------------------|
| **TC-016** | Validate NULL/blank fields in critical columns | i) Ensure no nulls in critical fields like `subject_to_physical_risk`, `o_over_s_amount`, `final_source` <br> ii) Count records with nulls in these columns |
| **TC-017** | Validate distinct values in categorical columns | i) List unique values in `final_source`, `high_acute`, `high_chronic`, `could_be_mapped` <br> ii) Check they match expected values (`Yes`, `No`, etc.) |
| **TC-018** | Validate record uniqueness by composite key | i) Define a business key (e.g. `ctpt_id`, `nuts_level_3_upper_clean_final`, `final_source`) <br> ii) Ensure no duplicates in `final_file_pblu` |
| **TC-019** | Validate coalescing hierarchy logic | i) For each hazard, check that if iteration_1 is null and iteration_2 is not, the final column picks iteration_2, and so on |
| **TC-020** | Validate exclusion logic for unmapped records | i) Check records where `could_be_mapped == 'No'` <br> ii) Confirm they are not used in aggregate stats or mapping logic |
| **TC-021** | Validate correct source-based riskno logic | i) If `final_source == 'collateral'` then `riskno` must be 1, etc. <br> ii) No cross-assigned riskno values |
| **TC-022** | Validate mapping coverage by geography | i) For all `country_name_upper_clean_final`, calculate mapping rate <br> ii) Share percentage of mapped vs unmapped |
| **TC-023** | Validate row_number deduplication logic | i) Check `row_number == 1` groupings still respect expected grouping constraints |
| **TC-024** | Validate ordering impact on row_number | i) Modify sorting column and check changes in resulting first-row selection |
| **TC-025** | Validate aggregated risk distribution | i) Count how many records fall under each `high_acute`, `high_chronic`, and `both_high` bucket for audit |

----
