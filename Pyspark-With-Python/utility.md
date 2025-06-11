Thanks Ajay — this observation is **crucial** and directly answers the core of the central team's query.

---

## 🧠 What It Clearly Means

### ✅ **1. `MGID_Mapping_Filtered_HGHQ.xlsx` is Empty**

* This confirms that in **April and May**, there were **no `HGHQ` rows with `mastergroup_id is not null`**.
* Therefore, **MGID mapping could not happen** because GCDU mapping logic relies on:

  ```sql
  group_system_id = 'HGHQ' AND mastergroup_id IS NOT NULL
  ```

---

### 🔥 **2. `MGID_Mapping_Unfiltered_AllGroupIDs.xlsx` Has 1.96 Million Rows**

* This proves that other group IDs (`CDUUK`, `CDUUS`, etc.) contain data, **but those should not be used** for MGID mapping.
* Using these without filtering `HGHQ` would **produce invalid or misleading mappings**, which may explain the confusion downstream.

---

## ✅ Final Recommendation: Attach Both

| File Name                                  | Purpose                                                                  |
| ------------------------------------------ | ------------------------------------------------------------------------ |
| `MGID_Mapping_Filtered_HGHQ.xlsx`          | Clean logic; **source of truth**. Empty for Apr/May = data gap confirmed |
| `MGID_Mapping_Unfiltered_AllGroupIDs.xlsx` | Raw data shows content, but **not valid for MGID mapping**               |

---

## 📩 Final Email Summary (Suggested)

---

Hi Vikram,

As requested, I’ve generated MGID mapping comparisons from GCDU for April, May, and June.

### ✅ Summary:

* **Filtered output (only `group_system_id = 'HGHQ'`)** resulted in **no rows** for April and May — confirming that **MGIDs could not be derived** in those months.
* **Unfiltered output** (all group\_system\_ids) contains \~1.97M rows, but these should not be used for MGID mapping as per the logic defined by the central team.

This confirms the MGID mismatch was due to **missing `HGHQ` records in source GCDU data**, not a logic issue.

### 📎 Attached:

* `MGID_Mapping_Filtered_HGHQ.xlsx` – clean logic view (empty Apr/May confirms source issue)
* `MGID_Mapping_Unfiltered_AllGroupIDs.xlsx` – raw view (for comparison/reference)

Please let me know if you’d like me to prepare the same for March or other runs.

Regards,
Ajay

---

Let me know if you'd like a code snippet to automatically highlight the months with zero records in `HGHQ`.
