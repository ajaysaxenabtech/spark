
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
Here’s a **refined and structured version** of the additional test cases with a focus on clarity, impact, and edge case coverage.

---

### **1️⃣ Data Integrity & Completeness Checks**
**1.1 Record Count Validation**  
✅ Verify that the final *Malta Counterparty CDA* contains the **same number of records** as *Malta Source Data* before joins and transformations.  
✅ Ensure that no records are **lost or duplicated** during the transformation process.  

**1.2 NULL Handling in Critical Fields**  
✅ Check that **mandatory fields** (e.g., `client_profile_id`, `line_of_business`) **do not have NULL values** post-processing.  
✅ Ensure that NULL values in **optional fields** (e.g., `bloomberg_id`, `lei`) are handled gracefully (either left blank or assigned defaults).  

**1.3 String Data Trimming in Joins**  
✅ Validate that **leading/trailing spaces in string-based join conditions** (e.g., CIN, GID) do not cause mismatches.  
✅ Ensure `TRIM()` is applied to relevant attributes before joining.  

---

### **2️⃣ Join & Relationship Testing**
**2.1 Handling of 1:N Relationships**  
✅ Verify that **one-to-many mappings (1:N)** (e.g., CIN → multiple GIDs) **do not cause data duplication** in the final dataset.  
✅ Ensure that cases where **a CIN has multiple Bloomberg IDs or LEIs** are properly eliminated or handled per business rules.  

**2.2 Duplicate Records Handling**  
✅ Test if duplicate records exist in *Malta Source Data* before processing.  
✅ Validate that the final dataset does **not introduce unintended duplicates** after joins and transformations.  

---

### **3️⃣ Transformation & Enhancement Validations**
**3.1 Derived Attribute Accuracy**  
✅ Validate that `client_profile_id`, `line_of_business`, and `eu_flag_final` are correctly derived according to **business rules**.  
✅ Ensure that `eu_flag_final` assignment **follows expected logic** (e.g., based on jurisdiction, legal entity type, or regulatory criteria).  

**3.2 CIS Mastergroup Processing**  
✅ Verify that **only "ACTIVE" mastergroups** are included (`mastergroup_status == 'ACTIVE'`).  
✅ Ensure the **filter on `booking_country == "MT"`** is correctly applied.  

**3.3 GID and LEI Mapping Validation**  
✅ Ensure that CIN → GID mapping **does not introduce duplicates**.  
✅ Check that CIN → LEI mapping removes **invalid 1:N relationships** (should ignore such cases).  

---

### **4️⃣ External Data Enhancements**
**4.1 Bloomberg ID Assignment Waterfall Logic**  
✅ Validate that the **waterfall logic correctly assigns Bloomberg ID** based on priority order (best available source first).  
✅ Test cases where **one CIN has multiple Bloomberg IDs** (should be ignored or resolved).  

**4.2 External Identifiers from GDA Data**  
✅ Ensure that cases where **BBG is NULL** are correctly handled (either assigned defaults or excluded).  
✅ Validate that **multiple BBGs for one CIN** do not lead to incorrect mappings.  

---

### **5️⃣ Performance & Edge Case Testing**
**5.1 Large Data Handling & Performance Testing**  
✅ Test **scalability** with large datasets to ensure joins and transformations **do not cause timeouts or failures**.  
✅ Measure execution time for **batch processing** to ensure it stays within acceptable limits.  

**5.2 Edge Cases Handling**  
✅ How does the system handle **a record with all NULL values** in its join keys?  
✅ What happens when a **new Malta counterparty appears** that was not present in previous datasets?  
✅ Validate system behavior when **external data sources (e.g., GDA, Bloomberg) are unavailable or return partial results**.  

---

### **Next Steps**
Would you like me to format this into a structured **test case document** (e.g., Excel, CSV, or Markdown format)? 🚀
---






