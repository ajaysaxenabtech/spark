

---
```python
# Creating a consolidated test plan for validating the final_file_pblu DataFrame

import pandas as pd

# Define test cases with description and validation steps
test_cases = [
    {
        "Test Case": "TC1",
        "Description": "Validate schema of final_file_pblu",
        "Validation Steps": "Check if all expected columns (e.g., subject_to_physical_risk, o_over_s_amount, riskno, etc.) exist in final_file_pblu"
    },
    {
        "Test Case": "TC2",
        "Description": "Validate distinct count of records against original dataset",
        "Validation Steps": "Ensure row count of final_file_pblu is less than or equal to the row count of pblu2 and unsecured_file4"
    },
    {
        "Test Case": "TC3",
        "Description": "Validate deduplication using row_number",
        "Validation Steps": "Ensure that final_file_pblu has only one row per group defined by partition logic (no duplicates based on business key)"
    },
    {
        "Test Case": "TC4",
        "Description": "Check values of 'riskno' column",
        "Validation Steps": "Ensure values in column 'riskno' are only 1 (collateral), 2 (loan), or 3 (other)"
    },
    {
        "Test Case": "TC5",
        "Description": "Validate 'der_risk' assignment",
        "Validation Steps": "Ensure der_risk is 1 when either high_acute_risk or high_chronic_risk is 1; otherwise 0"
    },
    {
        "Test Case": "TC6",
        "Description": "Validate 'subject_to_physical_risk' logic",
        "Validation Steps": "Ensure it's 'Yes' if any of high_acute_risk, high_chronic_risk, both_high_risk is 1; else 'No'"
    },
    {
        "Test Case": "TC7",
        "Description": "Validate coalesced hazard values",
        "Validation Steps": "Ensure columns like 'river_flood', 'urban_flood', etc., contain final coalesced values without nulls if any iteration data is present"
    },
    {
        "Test Case": "TC8",
        "Description": "Validate 'could_be_mapped' logic",
        "Validation Steps": "Check if it is 'Yes' when both acute_risks and chronic_risks are not null"
    },
    {
        "Test Case": "TC9",
        "Description": "Validate aggregation of o_over_s_amount",
        "Validation Steps": "Sum of o_over_s_amount grouped by subject_to_physical_risk and could_be_mapped should match total sum"
    },
    {
        "Test Case": "TC10",
        "Description": "Validate filtering for 'No' physical risk",
        "Validation Steps": "Distinct ctpt_id count when subject_to_physical_risk = 'No' should match manually filtered subset"
    }
]

df_test_cases = pd.DataFrame(test_cases)
import ace_tools as tools; tools.display_dataframe_to_user(name="Test Cases for final_file_pblu", dataframe=df_test_cases)

```


---
