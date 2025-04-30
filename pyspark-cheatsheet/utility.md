

---

**Build Logic for PBLU Final CDA**

---

### 🔧 Design Principles

The PBLU Final CDA consolidates physical risk indicators (acute and chronic) for counterparties across geographies, enabling downstream use for regulatory reporting and ESG risk analysis.

- **Primary Source**: PBLU parquet source file (e.g., `pblu_dec_ye_2024_v1.parquet`).
- **Record Count Integrity**: The final CDA must retain the record count integrity of the primary input (PBLU), subject to deduplication rules using partitioning logic.
- **Left Joins Only**: All enhancements from external datasets (e.g., RDH, NUTS, ThinkHazard) are joined using `LEFT JOIN` with PBLU data on the left.
- **Join Safety**:
  - Avoid overmatching from 1:N target datasets.
  - Pre-aggregate or filter external data to ensure 1:1 mappings on join keys.
- **TRIM All Strings Before Joins**: Apply `.trim()` on all string join keys to prevent mismatch due to white spaces.

---

### 🔄 Transformation Logic

#### Step 1: Pre-process PBLU Source
- Load PBLU source and drop technical attributes like `ingestion_date`
- Normalize column types (e.g., cast postcodes to strings)
- Derive: `loan_postcode2`, `collateral_postcode2`, `loan_country2`, `collateral_country2`, `ctpt_domcl_cnty_name`
- Derive `post_code_to_use`, `country_code_to_use`, and `final_source`

#### Step 2: Enhance With RDH
- Load RDH and map `countryCode` to `collateral_country2`
- Use LEFT JOIN on `collateral_country2 = countryCode`

#### Step 3: Map With NUTS
- Load NUTS (levels 1-3), derive `nuts1`, `nuts2`
- Join with PBLU on postcodes and country codes
- Handle string cleaning and accent removal for matching

#### Step 4: Map With ThinkHazard Data
- Load country, state, and district-level ThinkHazard datasets
- Clean `adm0_name`, `adm1_name`, `adm2_name`
- Rename risk flags (e.g., `fl → river_flood`)
- Apply suffix `_iteration_1/2/3` for district/state/country-level hazard data

#### Step 5: Coalescing Hazard Layers
- Prioritize hazards from lowest to highest granularity:
  - Use `coalesce()` to pick `iteration_1` > `iteration_2` > `iteration_3`
- Drop raw iteration columns after final hazard values are built

#### Step 6: Final France-specific Enhancement
- Load wildfire overlay file for France
- Join on department and region names using accent/whitespace-normalized matching
- Derive final wildfire values

#### Step 7: Flagging & Risk Attribution
- Derive:
  - `acute_risks` (concatenation of river, flood, quake, etc.)
  - `chronic_risks` (dry ground + extreme heat)
- Flags:
  - `high_acute_risk`, `high_chronic_risk`, `both_high_risk`
  - `subject_to_physical_risk`, `high_acute`, `high_chronic`, `both_high`
  - `could_be_mapped`

#### Step 8: Risk Number & Deduplication
- Assign `riskno`:
  - 1 = collateral, 2 = loan, 3 = others
- Deduplicate based on business keys with `row_number()` partition

#### Step 9: Final Output
- Drop intermediate columns (`row_number`, `*_iteration_n`)
- Output schema includes:
  - All derived hazard flags
  - Risk groupings
  - Source identifiers (`final_source`, `riskno`)

---

### 📆 Final Output: `final_file_pblu`
- Contains one row per counterparty/postcode/country group
- Flags for physical risk, mapping status, and risk classification
- Usable in ESG Reporting, Waterfall Allocation, and SREP Templates

---

Would you like this logic converted to a presentation format or included as part of data documentation in Excel or PDF?


----