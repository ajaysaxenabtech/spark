
---
### increasing the working width of notebook

```python
from IPython.display import display,HTML
display(HTML("<style>.container{width:100% !important; }</style>"))

```

---

```python
from IPython.display import display, HTML
display(HTML("<style>.CodeMirror { white-space: pre-wrap !important; }</style>"))
```
---

To wrap text inside a Python code cell in Jupyter Notebook (on JupyterHub), you can use the following approaches:

### 1. **Enable Line Wrapping in Jupyter Notebook Settings**
   - Open a new cell and run:
     ```python
     from notebook.services.config import ConfigManager
     cm = ConfigManager()
     cm.update('notebook', {"CodeCell": {"cm_config": {"lineWrapping": True}}})
     ```
   - This will enable text wrapping inside code cells.

### 2. **Modify Jupyter Notebook Configuration**
   If you have access to JupyterHub's configuration settings, you can enable line wrapping permanently:
   - Open JupyterHub terminal and edit the `custom.js` file:
     ```bash
     nano ~/.jupyter/custom/custom.js
     ```
   - Add the following lines:
     ```js
     require(["notebook/js/codecell"], function (codecell) {
         codecell.CodeCell.options_default.cm_config.lineWrapping = true;
     });
     ```
   - Save the file and restart JupyterHub.

### 3. **Use Keyboard Shortcuts (Temporary Solution)**
   - Click inside a code cell and press:
     - `Ctrl + Shift + P` → Search for **"Toggle Line Wrap"** and enable it.

### 4. **Modify `custom.css` (Alternative UI Method)**
   - If your JupyterHub allows custom CSS, create a file:
     ```bash
     nano ~/.jupyter/custom/custom.css
     ```
   - Add:
     ```css
     .CodeMirror pre {
         white-space: pre-wrap !important;
         word-break: break-word !important;
     }
     ```
   - Save and restart JupyterHub.

---

Below is a **structured test case document** in a format similar to the one in your screenshot. Each row represents a single test, with columns for **#Step**, **#Test Case**, and **#Expected Output**.

| **#Step**                                     | **#Test Case**                                                                                                                                   | **#Expected Output**                                                                                                                                                                 |
|-----------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **1.1 Record Count Validation**               | Verify that the final Malta Counterparty CDA contains the same number of records as Malta Source Data before joins and transformations.           | No records are lost or duplicated. Final record count matches the source data.                                                                                                       |
| **1.1 Record Count Validation**               | Ensure that no records are lost or duplicated during the transformation process.                                                                  | All source data is accounted for in the final dataset, with no extra records introduced.                                                                                             |
| **1.2 NULL Handling in Critical Fields**      | Check that mandatory fields (e.g., `client_profile_id`, `line_of_business`) do not have NULL values post-processing.                               | Mandatory fields are always populated. Any NULL in these fields should be flagged or corrected.                                                                                      |
| **1.2 NULL Handling in Critical Fields**      | Ensure that NULL values in optional fields (e.g., `bloomberg_id`, `lei`) are handled gracefully (either left blank or assigned defaults).          | Optional fields may be null or defaulted without causing downstream errors.                                                                                                          |
| **1.3 String Data Trimming in Joins**         | Validate that leading/trailing spaces in string-based join conditions (e.g., CIN, GID) do not cause mismatches.                                   | All relevant records match successfully, unaffected by whitespace.                                                                                                                   |
| **1.3 String Data Trimming in Joins**         | Ensure `TRIM()` is applied to relevant attributes before joining.                                                                                | Join keys are standardized, preventing data loss due to trailing or leading spaces.                                                                                                  |
| **2.1 Handling of 1:N Relationships**         | Verify that one-to-many mappings (e.g., CIN → multiple GIDs) do not cause data duplication in the final dataset.                                  | Records remain unique in the final output. No duplication arises from 1:N joins.                                                                                                     |
| **2.1 Handling of 1:N Relationships**         | Ensure that cases where a CIN has multiple Bloomberg IDs or LEIs are properly eliminated or handled per business rules.                            | Ambiguous 1:N relationships are resolved or excluded to maintain data integrity.                                                                                                     |
| **2.2 Duplicate Records Handling**            | Test if duplicate records exist in Malta Source Data before processing.                                                                          | Any duplicates in the source are identified and handled according to policy (e.g., flagged or deduplicated).                                                                         |
| **2.2 Duplicate Records Handling**            | Validate that the final dataset does not introduce unintended duplicates after joins and transformations.                                         | No new duplicates appear in the final dataset.                                                                                                                                       |
| **3.1 Derived Attribute Accuracy**            | Validate that `client_profile_id`, `line_of_business`, and `eu_flag_final` are correctly derived according to business rules.                      | Derived fields match the logic defined in the Asset Schema or relevant business documentation.                                                                                       |
| **3.1 Derived Attribute Accuracy**            | Ensure that `eu_flag_final` assignment follows expected logic (e.g., based on jurisdiction, legal entity type, or regulatory criteria).           | The correct EU flag status is set for each record as defined by business/regulatory rules.                                                                                           |
| **3.2 CIS Mastergroup Processing**            | Verify that only "ACTIVE" mastergroups are included (`mastergroup_status == 'ACTIVE'`).                                                           | Records with inactive mastergroups are excluded from the final dataset.                                                                                                              |
| **3.2 CIS Mastergroup Processing**            | Ensure the filter on `booking_country == "MT"` is correctly applied.                                                                              | Only records relevant to Malta remain after filtering.                                                                                                                               |
| **3.3 GID and LEI Mapping Validation**        | Ensure that CIN → GID mapping does not introduce duplicates.                                                                                     | A single CIN corresponds to a single GID, or ambiguous cases are ignored per rules.                                                                                                  |
| **3.3 GID and LEI Mapping Validation**        | Check that CIN → LEI mapping removes invalid 1:N relationships (should ignore such cases).                                                        | No 1:N LEI assignments remain in the final dataset.                                                                                                                                  |
| **4.1 Bloomberg ID Assignment Waterfall Logic** | Validate that the waterfall logic correctly assigns Bloomberg ID based on priority order (best available source first).                          | Bloomberg IDs are populated from the highest-priority source, with fallback used if the primary source is unavailable.                                                               |
| **4.1 Bloomberg ID Assignment Waterfall Logic** | Test cases where one CIN has multiple Bloomberg IDs (should be ignored or resolved).                                                              | Ambiguous Bloomberg IDs do not cause duplication or conflict in the final dataset.                                                                                                   |
| **4.2 External Identifiers from GDA Data**    | Ensure that cases where BBG is NULL are correctly handled (either assigned defaults or excluded).                                                 | NULL BBG IDs are properly accounted for, preventing data mismatch or errors.                                                                                                         |
| **4.2 External Identifiers from GDA Data**    | Validate that multiple BBGs for one CIN do not lead to incorrect mappings.                                                                        | Only the correct BBG is used, or ambiguous records are excluded.                                                                                                                     |
| **5.1 Large Data Handling & Performance**     | Test scalability with large datasets to ensure joins and transformations do not cause timeouts or failures.                                      | The system processes large data within acceptable performance thresholds.                                                                                                            |
| **5.1 Large Data Handling & Performance**     | Measure execution time for batch processing to ensure it stays within acceptable limits.                                                         | Processing completes within the defined SLA or time window.                                                                                                                          |
| **5.2 Edge Cases Handling**                   | How does the system handle a record with all NULL values in its join keys?                                                                       | Such records are either excluded or flagged, preventing join failures.                                                                                                               |
| **5.2 Edge Cases Handling**                   | What happens when a new Malta counterparty appears that was not present in previous datasets?                                                    | New records are processed without errors and assigned appropriate IDs.                                                                                                               |
| **5.2 Edge Cases Handling**                   | Validate system behavior when external data sources (e.g., GDA, Bloomberg) are unavailable or return partial results.                            | The system gracefully handles missing external data, continuing with partial or fallback logic.                                                                                      |

> **Tip:**  
> - You can copy and paste this table directly into a Confluence page, Excel file, or any other documentation tool.  
> - Adjust any terminology or details (e.g., “MT” vs. “Malta”) as needed to align with your project standards.

---






