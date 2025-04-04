
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


Yes, we **can** and **should** include **Final Output Validation** to ensure financial data consistency, especially for **balance sheet integrity**. Below is how we can add this test case to the document:

| **#Step** | **#Test Case** | **#Expected Output** |
|-----------|--------------|----------------------|
| **6.1 Final Output Validation** | Verify that the **Balance Sheet column sum** in the transformed data matches the corresponding sum in the source/input data. | The total sum of balance sheet columns in the transformed dataset should **exactly** match the input data, ensuring no loss, duplication, or miscalculation. |
| **6.1 Final Output Validation** | Ensure that all debits and credits in financial reports are correctly retained and balanced post-transformation. | Debit = Credit at all processing stages, maintaining financial consistency. |
| **6.2 Data Type and Rounding Consistency** | Verify that **numeric precision** and rounding rules are maintained after transformations. | No unexpected truncation, rounding errors, or precision loss in final output. |

Would you like me to refine this further based on any **specific financial columns** that need validation? 🚀
---






 