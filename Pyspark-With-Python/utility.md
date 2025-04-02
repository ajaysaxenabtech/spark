
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

---



### **Step 2: Identify New Test Cases**  
- **Gap Analysis in Existing Scenarios:**  
  - Validate **record count parity** with Malta Source Data.  
  - Ensure **JOIN integrity** (LEFT JOINS, unique keys, TRIM applied).  
  - Check **derived attribute calculations** (e.g., logic behind `eu_flag_final`).  
  - Verify **CIS Mastergroup Mapping** for active records.  
  - Validate **GID uniqueness** (ignore 1:N cases).  
  - Ensure **External Identifiers (LEI/Bloomberg ID)** are correctly mapped.  

- **New Test Case Suggestions:**  
  - **Completeness Checks:**  
    - Ensure every record has `client_profile_id`, `final_counterparty_name`, and `lei_malta`.  
  - **Accuracy & Consistency Checks:**  
    - Verify `mastergroup_status` is only ‘ACTIVE’.  
    - Check if LEI is sourced from `Wholesale Party Identifiers`.  
  - **Timeliness Checks:**  
    - Validate that the latest CIS & GCDU data are used.  
  - **Business Rule Validation:**  
    - `eu_flag_final` must be correctly derived based on business rules.  



