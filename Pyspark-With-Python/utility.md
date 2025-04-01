
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


----


### French Gap Analysis Summary

#### Key Observations:
1. **mastergroup_id_gcdu2**  
   - **51.48%** of CINs (3,608) do not exist in this field, accounting for **70.79%** of the total exposure (€105.44 billion).  
   - **48.52%** of CINs (3,401) exist, representing **29.21%** of the exposure (€43.50 billion).  

2. **mastergroup_ids**  
   - **33.71%** of CINs (2,363) are missing, covering **30.74%** of exposure (€45.79 billion).  
   - **66.29%** of CINs (4,646) exist, contributing **69.26%** of exposure (€103.15 billion).  

3. **mastergroup_name_gcdu2**  
   - Mirrors the results of **mastergroup_id_gcdu2**, with identical percentages and exposure values.  

4. **mastergroup_names**  
   - Matches **mastergroup_ids** in distribution, with **66.29%** of CINs existing and covering **69.26%** of exposure.  

5. **cis_mastergroup_id**  
   - **66.07%** of CINs (4,631) are missing, representing **39.18%** of exposure (€58.36 billion).  
   - **33.93%** of CINs (2,378) exist, accounting for **60.82%** of exposure (€90.58 billion).  

6. **mastergroup_ids vs. cis_mastergroup_id Comparison**  
   - **Nearly all records (7,006 CINs)** show differences between these fields, impacting **€148.71 billion** in exposure.  
   - Only **3 CINs** have identical values, with minimal exposure (€23.98 million).  

#### Implications:  
- Significant data gaps exist, especially in **mastergroup_id_gcdu2** and **cis_mastergroup_id**, where missing values cover large portions of exposure.  
- Discrepancies between **mastergroup_ids** and **cis_mastergroup_id** suggest alignment challenges in identifier mapping.  
- Prioritizing consistency between these fields could improve data reliability for reporting or compliance (e.g., NFRD).  

**Total Exposure:** €149.94 billion (sum of all analyzed segments).  

---  
*Note: Percentages are approximate due to rounding. Exposure values in euros (€).*


---

### Germany Gap Analysis Summary  

#### Key Observations:  
1. **mastergroup_id**  
   - **38.39%** of BP IDs (1,100) and **38.57%** of GIDs (1,097) are missing, accounting for **14.25%** of total exposure (€-4.79 billion).  
   - **61.61%** of BP IDs (1,765) and **61.43%** of GIDs (1,747) exist, covering **85.75%** of exposure (€-28.81 billion).  

2. **mastergroup_name**  
   - Identical distribution to **mastergroup_id**, with the same missing/existing percentages and exposure values.  

3. **cis_mastergroup_id**  
   - **93.16%** of BP IDs (2,683) and **93.18%** of GIDs (2,664) are missing, representing **57.03%** of exposure (€-19.16 billion).  
   - Only **6.84%** of BP IDs (197) and **6.82%** of GIDs (195) exist, yet they cover **42.97%** of exposure (€-14.44 billion).  

4. **mastergroup_id vs. cis_mastergroup_id Comparison**  
   - **Nearly all records (2,865 BP IDs)** show discrepancies between these fields, impacting **€-33.6 billion** in exposure.  
   - Minimal alignment (exact count not provided) with negligible exposure impact.  

#### Implications:  
- **High Data Gaps**: Critical missing values in **cis_mastergroup_id** (93% missing) despite covering 43% of exposure, suggesting reliance on a small subset of valid identifiers.  
- **Consistency Issues**: Mismatches between **mastergroup_id** and **cis_mastergroup_id** highlight potential mapping errors or divergent data sources.  
- **Exposure Concentration**: Existing identifiers in **mastergroup_id** and **cis_mastergroup_id** dominate exposure (86% and 43%, respectively), necessitating validation for risk assessment.  

**Total Exposure:** €-33.6 billion (sum of discrepant records).  

---  
*Note: Negative values (e.g., €-4.79B) likely represent liabilities or debit balances. Percentages are approximate due to rounding.*

---


### Malta Gap Analysis Summary  

#### Key Observations:  
1. **master_group_id**  
   - **9.87%** of client profiles (5,646) are missing, accounting for **57.10%** of total exposure (€388.9 million).  
   - **90.13%** of client profiles (51,530) exist, covering **42.90%** of exposure (€292.1 million).  

2. **mastergroup_id**  
   - Nearly identical to **master_group_id**, with **9.79%** missing (5,597 profiles) and **90.21%** existing (51,579 profiles), and the same exposure distribution (57.10% and 42.90%).  

3. **mastergroup_name**  
   - **99.00%** of client profiles (56,603) are missing, representing **88.61%** of exposure (€603.5 million).  
   - Only **1.00%** of profiles (573) exist, covering **11.39%** of exposure (€77.5 million).  

4. **mastergroupid**  
   - Mirrors **mastergroup_id** and **master_group_id**, with the same missing/existing percentages and exposure values.  

5. **cis_mastergroup_id**  
   - **98.87%** of client profiles (56,528) are missing, accounting for **88.53%** of exposure (€602.9 million).  
   - **1.13%** of profiles (648) exist, representing **11.47%** of exposure (€78.1 million).  

6. **mastergroup_id vs. cis_mastergroup_id Comparison**  
   - **All records (57,176 client profiles)** show discrepancies between these fields, impacting **€681.0 million** in exposure.  
   - No identical records were found.  

#### Implications:  
- **Critical Data Gaps**: Extremely high missing rates for **mastergroup_name** and **cis_mastergroup_id** (99% and 98.87%, respectively), despite their significant exposure coverage (88.61% and 88.53%).  
- **Identifier Consistency**: The fields **master_group_id**, **mastergroup_id**, and **mastergroupid** show identical distributions, suggesting potential redundancy or systematic data entry issues.  
- **Exposure Concentration**: A small fraction of existing identifiers (<2%) controls a disproportionate share of exposure (11-42%), warranting validation for risk management.  

**Total Exposure:** €681.0 million (sum of discrepant records).  

---  
*Note: Exposure values in euros (€). Percentages are approximate due to rounding.*