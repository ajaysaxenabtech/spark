
### increasing the working width of notebook

```python
from IPython.display import display,HTML
display(HTML("<style>.container{width:100% !important; }</style>"))

```

To wrap text inside a cell in Jupyter Notebook, you can use **HTML and CSS** in a Markdown cell or modify the CSS styles of the output.

### **Method 1: Using HTML in a Markdown Cell**
You can use the following HTML and CSS inside a Markdown cell:

```html
<div style="word-wrap: break-word; width: 500px;">
    This is a long text that should wrap inside the div container without overflowing the cell.
</div>
```

This ensures that text within the `div` does not overflow.

---

### **Method 2: Changing Cell Output Styling**
If you want to wrap text inside the **output cells**, modify the Jupyter Notebook’s CSS by running the following in a code cell:

```python
from IPython.core.display import display, HTML

display(HTML("<style>.output_area pre { white-space: pre-wrap; }</style>"))
```

This will ensure that long text in the output is wrapped instead of overflowing.

---

### **Method 3: Using Pandas for Wrapping Text in DataFrames**
If you are displaying a Pandas DataFrame and want text to wrap inside the table:

```python
import pandas as pd

df = pd.DataFrame({'Column1': ['This is a long text that should wrap inside the cell instead of overflowing.']})

# Apply CSS to wrap text
df.style.set_properties(**{'white-space': 'pre-wrap'})
```
---

If your Python code is overflowing horizontally in a Jupyter Notebook **code cell**, you can wrap it inside the cell by modifying Jupyter's CSS.

### **Method 1: Temporarily Apply CSS in the Notebook**
Run the following command inside a **code cell** to enable text wrapping:

```python
from IPython.core.display import display, HTML

display(HTML("<style>.CodeMirror pre { white-space: pre-wrap; word-break: break-word; }</style>"))
```

This will make long lines wrap inside the code cell instead of creating horizontal scrollbars.

---

### **Method 2: Permanently Change Jupyter Notebook's CSS**
If you want this change to persist across all notebooks, follow these steps:

1. **Locate Jupyter's Custom CSS Directory:**
   Open a terminal (or use a Jupyter Notebook cell) and run:

   ```bash
   jupyter --config-dir
   ```

   This will give you the path where Jupyter's settings are stored.

2. **Edit or Create `custom.css`:**
   - Navigate to `{config-dir}/custom/` (create it if it doesn't exist).
   - Inside this folder, create or edit `custom.css`.

3. **Add the Following CSS:**
   Open `custom.css` in a text editor and add:

   ```css
   .CodeMirror pre {
       white-space: pre-wrap !important;
       word-break: break-word !important;
   }
   ```

4. **Restart Jupyter Notebook:**  
   After saving the file, restart Jupyter Notebook for changes to take effect.

---

### **Method 3: Enable Line Wrapping in JupyterLab**
If you're using **JupyterLab**, you can enable wrapping from the UI:

1. **Go to** `Settings` → `Advanced Settings Editor`.
2. In the left panel, select `Notebook`.
3. Search for `codeCellConfig`.
4. Set `"lineWrapping": true`.
5. Click **Save** and restart JupyterLab.

This will enable automatic wrapping inside code cells.

---

To wrap text inside a code cell in **Jupyter Notebook** (running on JupyterHub), you can use either **CSS styling** or **IPython display utilities**. Here are the methods:

### **Method 1: Using CSS Styling (Permanent for a Notebook)**
You can inject custom CSS to enable text wrapping in code cells.

1. **Create a new cell** and run:
   ```python
   from IPython.display import HTML, display

   display(HTML("""
   <style>
       .CodeMirror pre {
           white-space: pre-wrap;
           word-break: break-all;
       }
   </style>
   """))
   ```
   - This will enable **text wrapping** for all code cells in the notebook.

2. **Alternatively**, if you only want to wrap text in a **specific output cell**, use:
   ```python
   from IPython.display import HTML
   HTML("<style>div.output_area pre { white-space: pre-wrap; }</style>")
   ```

---

### **Method 2: Using `%%html` Magic (Temporary)**
If you want a quick solution for a single notebook session:
```python
%%html
<style>
    .CodeMirror pre {white-space: pre-wrap;}
</style>
```

---

### **Method 3: Adjusting Notebook Settings (Persistent)**
If you have access to JupyterHub's configuration, you can modify the `custom.css` file:

1. **Find Jupyter's CSS directory** by running in a notebook:
   ```python
   import jupyter_core
   jupyter_core.paths.jupyter_config_dir()
   ```
2. **Create or edit** `~/.jupyter/custom/custom.css` and add:
   ```css
   .CodeMirror pre {
       white-space: pre-wrap !important;
   }
   ```
3. **Restart** JupyterHub for changes to take effect.

---

### **Method 4: Using `IPython.display` for Specific Outputs**
If you want to wrap **only specific long outputs**, use:
```python
from IPython.display import display, HTML
display(HTML("<div style='white-space: pre-wrap;'>Your long text here...</div>"))
```

---

### **Which Method to Choose?**
- **For a single notebook**: Use **Method 1 or 2**.
- **For all notebooks**: Use **Method 3** (if you have admin access).
- **For specific outputs**: Use **Method 4**.


