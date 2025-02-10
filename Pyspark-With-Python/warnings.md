
### WARN window.Window.Exec: No Partition Defined for Window operation! Moving all data to a single partitings

1. Set Log Level to ERROR<br>
You can reduce the log level from WARN to ERROR. This way, only errors are shown, and warnings are suppressed.

```
spark.sparkContext.setLogLevel("ERROR")
```

2. Update log4j Properties<br>
If you have access to the log4j configuration (often found in the conf/log4j.properties file in your Spark setup), you can adjust the settings:

Add or modify the following line:
```
log4j.logger.org.apache.spark.sql.execution.window.WindowExec=ERROR
```

3. Programmatically Using spark-defaults.conf<br>
If you prefer not to modify the configuration files, you can set the log level programmatically when initializing your Spark session:
```
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.sql.execution.window.WindowExec", "ERROR") \
    .getOrCreate()
```

---

```powershell
Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;

public class Keyboard {
    [DllImport("user32.dll", CharSet = CharSet.Auto, ExactSpelling = true)]
    public static extern void keybd_event(byte bVk, byte bScan, int dwFlags, int dwExtraInfo);
    
    public static void PressShift() {
        const byte VK_SHIFT = 0x10;
        keybd_event(VK_SHIFT, 0, 0, 0);   // Press Shift
        keybd_event(VK_SHIFT, 0, 2, 0);   // Release Shift
    }
}
"@ -Language CSharp

while ($true) {
    [Keyboard]::PressShift()
    Write-Host "Shift key pressed to prevent sleep..." -ForegroundColor Green
    Start-Sleep -Seconds 60  # Adjust time as needed
}

```
---

`rundll32.exe powrprof.dll,SetSuspendState Sleep`

---


### **Comparison: GDA vs. MDA vs. CDA**  

HSBC’s **Reusable Data Assets** are categorized into three key types:  

1️⃣ **Global Data Asset (GDA)**  
2️⃣ **Master Data Asset (MDA)**  
3️⃣ **Contextual Data Asset (CDA)**  

Each plays a distinct role in ensuring **data consistency, governance, and usability** across different business processes.  

---

## **1️⃣ Global Data Asset (GDA) 🌎**  
✅ **Definition:**  
A **centrally governed and reusable** data asset that serves multiple business functions **across HSBC globally**.  

✅ **Purpose:**  
Ensures **standardization** and **reusability** of critical data elements **across all HSBC entities and regions**.  

✅ **Examples:**  
🔹 Customer Identifiers (e.g., Global Customer ID, HSBC Client Number)  
🔹 Universal Product Codes (e.g., Credit Cards, Mortgages, Loan Types)  
🔹 Standardized Country Codes & Currency Codes (e.g., ISO 3166, ISO 4217)  
🔹 Risk Ratings & Compliance Indicators (e.g., Sanctions Lists, AML Flags)  

✅ **Key Characteristics:**  
🔹 Used by **multiple regions and business functions**.  
🔹 Follows strict **global governance** and **data quality standards**.  
🔹 Changes require **centralized approval**.  

✅ **Analogy:**  
🛠 Think of GDA as a **universal foundation**—like the **International Financial Reporting Standards (IFRS)** in accounting. It ensures every HSBC entity follows the same core data principles.  

---

## **2️⃣ Master Data Asset (MDA) 🏛**  
✅ **Definition:**  
A **trusted, high-quality, and consistent** dataset that serves as a **single source of truth** for a specific domain (e.g., Customer, Product, Counterparty).  

✅ **Purpose:**  
Eliminates **data duplication** and **inconsistencies** across different HSBC systems.  

✅ **Examples:**  
🔹 **Customer Master Data** → Name, Address, Date of Birth, Risk Profile  
🔹 **Product Master Data** → Product ID, Product Type, Features  
🔹 **Counterparty Master Data** → Vendor Information, Third-party Data  

✅ **Key Characteristics:**  
🔹 Serves **specific domains** (e.g., Customer, Product, Counterparty).  
🔹 Maintained through **rigorous data governance** processes.  
🔹 Helps HSBC ensure **data accuracy, consistency, and reliability**.  

✅ **Analogy:**  
📖 Think of MDA as a **well-maintained dictionary**—it provides a **definitive reference** for key business entities, preventing duplicate or conflicting records.  

---

## **3️⃣ Contextual Data Asset (CDA) 🎯**  
✅ **Definition:**  
A **business-specific data asset** tailored for **specific use cases, regions, or business units**.  

✅ **Purpose:**  
Provides **customized** and **contextualized** insights based on **business needs**.  

✅ **Examples:**  
🔹 **Customer Risk Score for a Specific Region** → Tailored for HSBC India vs. HSBC UK  
🔹 **Market Trends & Sentiment Analysis** → Contextual insights for investments  
🔹 **Business Unit-Specific KPIs** → Data assets designed for Retail Banking vs. Corporate Banking  

✅ **Key Characteristics:**  
🔹 Built on top of **GDA and MDA** but tailored for **specific HSBC use cases**.  
🔹 Allows for **localization and business unit customization**.  
🔹 More flexible in governance compared to GDA/MDA.  

✅ **Analogy:**  
🎭 Think of CDA as a **customized news report**—it presents **only the relevant information** for a particular business unit or region, rather than a one-size-fits-all approach.  

---

## **🆚 Summary: Key Differences**  

| Feature             | **GDA (Global Data Asset)** | **MDA (Master Data Asset)** | **CDA (Contextual Data Asset)** |
|---------------------|--------------------------|---------------------------|----------------------------|
| **Scope**          | HSBC-wide (Global)       | Domain-Specific           | Business-Specific         |
| **Governance**     | Strictly Controlled      | High Governance           | Moderate Governance       |
| **Usage**         | Shared Across HSBC      | Single Source of Truth for Domains | Customized for Business Units |
| **Examples**       | Global Customer ID, ISO Codes | Customer Master Data, Product Master Data | Regional Risk Scores, Market Insights |
| **Flexibility**    | Low (Strict Standards)   | Medium (Single Source of Truth) | High (Tailored for Use Cases) |

---

## **🔍 Practical Example: How They Work Together**  
Imagine HSBC wants to assess a customer's creditworthiness.  

1️⃣ **GDA (Global Data Asset)** → Provides a **unique, global customer ID** that HSBC uses across all regions.  
2️⃣ **MDA (Master Data Asset)** → Stores **verified customer details**, including name, address, and financial history.  
3️⃣ **CDA (Contextual Data Asset)** → Generates a **localized risk score** based on regional economic conditions and transaction patterns.  

🔹 **Outcome:** HSBC can **accurately assess the customer’s credit risk** while ensuring **data consistency and compliance** across regions.  

---

### **📝 Final Takeaways**  
✅ **GDA** ensures **global standardization**.  
✅ **MDA** provides a **single source of truth**.  
✅ **CDA** delivers **customized, business-specific insights**.  

----


----


## **Global Data Asset (GDA) vs. Master Data Asset (MDA) vs. Contextual Data Asset (CDA)**

HSBC's **Reusable Data Assets** are classified into three key types to ensure **data consistency, governance, and usability** across various business processes. This document provides a structured comparison of **Global Data Asset (GDA), Master Data Asset (MDA), and Contextual Data Asset (CDA)**.

---

### **1️⃣ Global Data Asset (GDA)** 🌎

#### **Definition**
A centrally governed and reusable data asset that serves multiple business functions **globally** across HSBC.

#### **Purpose**
Ensures **standardization** and **reusability** of critical data elements across all HSBC entities and regions.

#### **Examples**
- **Customer Identifiers** (e.g., Global Customer ID, HSBC Client Number)
- **Universal Product Codes** (e.g., Credit Cards, Mortgages, Loan Types)
- **Standardized Country & Currency Codes** (e.g., ISO 3166, ISO 4217)
- **Risk Ratings & Compliance Indicators** (e.g., Sanctions Lists, AML Flags)

#### **Key Characteristics**
✔ Used by **multiple regions and business functions**
✔ Follows strict **global governance** and **data quality standards**
✔ Changes require **centralized approval**

#### **Analogy**
Think of GDA as **International Financial Reporting Standards (IFRS)**—a universal foundation ensuring that all HSBC entities follow the same core data principles.

---

### **2️⃣ Master Data Asset (MDA)** 🏛

#### **Definition**
A **trusted, high-quality, and consistent dataset** that serves as a **single source of truth** for a specific domain (e.g., Customer, Product, Counterparty).

#### **Purpose**
Eliminates **data duplication** and **inconsistencies** across different HSBC systems.

#### **Examples**
- **Customer Master Data** → Name, Address, Date of Birth, Risk Profile
- **Product Master Data** → Product ID, Product Type, Features
- **Counterparty Master Data** → Vendor Information, Third-party Data

#### **Key Characteristics**
✔ Serves **specific domains** (e.g., Customer, Product, Counterparty)
✔ Maintained through **rigorous data governance** processes
✔ Ensures **data accuracy, consistency, and reliability**

#### **Analogy**
Think of MDA as a **well-maintained dictionary**—a definitive reference preventing duplicate or conflicting records.

---

### **3️⃣ Contextual Data Asset (CDA)** 🎯

#### **Definition**
A **business-specific data asset** tailored for **specific use cases, regions, or business units**.

#### **Purpose**
Provides **customized** and **contextualized** insights based on **business needs**.

#### **Examples**
- **Customer Risk Score for a Specific Region** → Tailored for HSBC India vs. HSBC UK
- **Market Trends & Sentiment Analysis** → Contextual insights for investments
- **Business Unit-Specific KPIs** → Data assets designed for Retail Banking vs. Corporate Banking

#### **Key Characteristics**
✔ Built on top of **GDA and MDA** but tailored for **specific HSBC use cases**
✔ Allows for **localization and business unit customization**
✔ More flexible in governance compared to GDA/MDA

#### **Analogy**
Think of CDA as a **customized news report**—presenting **only the relevant information** for a particular business unit or region.

---

## **🆚 Summary: Key Differences**

| Feature             | **GDA (Global Data Asset)** | **MDA (Master Data Asset)** | **CDA (Contextual Data Asset)** |
|---------------------|--------------------------|---------------------------|----------------------------|
| **Scope**          | HSBC-wide (Global)       | Domain-Specific           | Business-Specific         |
| **Governance**     | Strictly Controlled      | High Governance           | Moderate Governance       |
| **Usage**         | Shared Across HSBC      | Single Source of Truth for Domains | Customized for Business Units |
| **Examples**       | Global Customer ID, ISO Codes | Customer Master Data, Product Master Data | Regional Risk Scores, Market Insights |
| **Flexibility**    | Low (Strict Standards)   | Medium (Single Source of Truth) | High (Tailored for Use Cases) |

---

## **🔍 Practical Example: How They Work Together**

Imagine HSBC wants to assess a customer's creditworthiness:

1️⃣ **GDA (Global Data Asset)** → Provides a **unique, global customer ID** that HSBC uses across all regions.  
2️⃣ **MDA (Master Data Asset)** → Stores **verified customer details**, including name, address, and financial history.  
3️⃣ **CDA (Contextual Data Asset)** → Generates a **localized risk score** based on regional economic conditions and transaction patterns.  

✔ **Outcome:** HSBC can **accurately assess the customer’s credit risk** while ensuring **data consistency and compliance** across regions.

---

## **📝 Final Takeaways**
✅ **GDA** ensures **global standardization**.  
✅ **MDA** provides a **single source of truth**.  
✅ **CDA** delivers **customized, business-specific insights**.  

📌 This structured approach enables HSBC to **streamline data management, enhance decision-making, and ensure regulatory compliance** across all its business units.

---

## **📢 Questions & Feedback**
For any clarifications or further details, feel free to reach out via the **Data Governance Team** on Confluence!

---