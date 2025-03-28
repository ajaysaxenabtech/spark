
---
### increasing the working width of notebook

```python
from IPython.display import display,HTML
display(HTML("<style>.container{width:100% !important; }</style>"))

```

---



In **Jupyter Notebook (JupyterHub)**, text wrapping inside code cells is not enabled by default, but you can enable it using **CSS overrides** or **custom configurations**.

---

### **Method 1: Using CSS (Temporary for Current Session)**
Run the following in a **Markdown cell**:

```html
<style>
    .CodeMirror pre {
        white-space: pre-wrap !important;
        word-wrap: break-word !important;
    }
</style>
```
- This will wrap text in all code cells, but it will reset when you restart the notebook.

---

### **Method 2: Modifying Jupyter's Custom CSS (Persistent)**
1. **Find Jupyter's custom CSS directory** by running:
   ```bash
   jupyter --config-dir
   ```
   It will return a path like:
   ```
   /home/user/.jupyter
   ```
2. **Create or edit `custom.css`** inside:
   ```
   ~/.jupyter/custom/custom.css
   ```
3. Add the following CSS:
   ```css
   .CodeMirror pre {
       white-space: pre-wrap !important;
       word-wrap: break-word !important;
   }
   ```
4. Restart JupyterHub to apply changes.

---

### **Method 3: Using nbextensions (If Enabled)**
If **Jupyter Notebook Extensions** are available, you can install `code_prettify`:
1. Install it:
   ```bash
   pip install jupyter_contrib_nbextensions
   jupyter contrib nbextension install --user
   ```
2. Enable it:
   ```bash
   jupyter nbextension enable code_prettify/main
   ```
3. Restart JupyterHub.

---

### **Alternative: Adjusting Display Width (Workaround)**
You can set Jupyter to **wrap output text** but not the code inside the cell:
```python
from IPython.core.display import display, HTML
display(HTML("<style>.container { width:80% !important; }</style>"))
```
This makes the Jupyter window wider, helping avoid horizontal scrolling.

---

Let me know which method works for you or if you need a **custom script** for auto-applying these changes in JupyterHub! 🚀