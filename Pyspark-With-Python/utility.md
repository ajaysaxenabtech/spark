

Certainly, here is a concise yet clear documentation **targeted at someone with strong advanced Excel skills** but perhaps less experience with PySpark, focusing first on the generic decimal conversion and then on the specific transformations applied to each attribute.

---

## Transformation Documentation

### **Step 1: Converting All `decimal(38,9)` Columns to Text with 9 Decimal Places**

* **What happens:**
  Every column in the dataset that is of type `decimal(38,9)` is converted to a text (string) format with exactly 9 digits after the decimal point.
* **Why:**
  This prevents loss of precision and ensures numbers are not displayed in scientific notation or with rounding issues when exported (e.g., to CSV or Excel).
* **Excel Analogy:**
  Similar to applying a number format like `"0.000000000"` and then converting the column to "Text" to lock the display format.

---

### **Step 2: Attribute-wise Transformation Description**

Below are the individual attribute transformations, with their logic described in Excel-like language:

---

#### **mipaicm**

* **Transformation:**
  Converted to a string (if not already), then wrapped in double quotes (`"`).
* **Purpose:**
  To preserve any leading zeros, special characters, or large numbers during export.
* **Excel Analogy:**
  `=CHAR(34) & TEXT([@mipaicm], "General") & CHAR(34)`

---

#### **iarrsys**

* **Transformation:**
  Value wrapped in double quotes.
* **Purpose:**
  To maintain text integrity during export.
* **Excel Analogy:**
  `=CHAR(34) & [@iarrsys] & CHAR(34)`

---

#### **iipaalt**

* **Transformation:**
  Value wrapped in double quotes.
* **Purpose:**
  Same as above, for text preservation.
* **Excel Analogy:**
  `=CHAR(34) & [@iipaalt] & CHAR(34)`

---

#### **lei**

* **Transformation:**
  Value wrapped in double quotes.
* **Purpose:**
  Same as above, for text preservation.
* **Excel Analogy:**
  `=CHAR(34) & [@lei] & CHAR(34)`

---

#### **marrnmv**

* **Transformation:**
  First, converted from string to number, formatted as a whole number (no decimals), then converted back to string, and finally wrapped in double quotes.
* **Purpose:**
  Ensures numbers like `"000123"` are preserved as `"123"`, but with quotes to maintain the string format during export.
* **Excel Analogy:**
  `=CHAR(34) & TEXT(VALUE([@marrnmv]), "0") & CHAR(34)`

---

#### **mcrdcls**

* **Transformation:**
  Same as `marrnmv`: number formatting as integer, then wrapped in quotes.
* **Purpose:**
  To avoid scientific notation and preserve as plain text.
* **Excel Analogy:**
  `=CHAR(34) & TEXT(VALUE([@mcrdcls]), "0") & CHAR(34)`

---

#### **crn**

* **Transformation:**
  Converted from string to number, formatted as whole number, then wrapped in quotes.
* **Purpose:**
  Same as above, for export safety.
* **Excel Analogy:**
  `=CHAR(34) & TEXT(VALUE([@crn]), "0") & CHAR(34)`

---

### **Final Note**

* **Overall Purpose:**
  All these transformations are performed to ensure that, when the data is exported to CSV (or opened in Excel), the formatting is preserved, numbers are not auto-converted to scientific notation, and text fields retain all their characters exactly as present in the source.

---

**Let me know if you want this as a formal document (Word/PDF), or if you want a table format summarizing all transformations for sharing with non-technical team members.**
