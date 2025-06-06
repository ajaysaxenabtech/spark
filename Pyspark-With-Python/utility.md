Below is the professional documentation for **Step 2.8 (Egress dataset to shared drive in CSV format)** based on the scripts shown in images 3, 4, and 5. The writeup aligns with your previous documentation style:

---

### Step 2.8: Egress Dataset to Shared Drive in CSV Format

#### Objective

To export the processed datasets from HDFS to a designated shared drive in CSV format, ensuring accessibility for downstream processes and audit requirements.

#### Implementation Steps

1. **Prepare and Format Data for Egress**

   * Apply necessary transformations to all columns based on their data types (decimal, integer, string, timestamp, date) using custom UDFs.
   * Use a dispatcher function (`transform_column`) to automate type-based transformation for each field in the schema.
   * Ensure all fields are properly formatted as strings to comply with CSV output standards.

2. **Dynamic Egress Script**

   * Iterate over the list of source file paths and names (`path_list`), reading each parquet file using Spark.
   * Apply the defined transformations to columns and select the formatted columns.
   * Export each dataframe to a local CSV file with a timestamp in its filename for traceability.
   * Use the `hdfs dfs -put` command to move the local CSV file to the appropriate HDFS directory for external disclosures.

   ```python
   for file in path_list:
       kpi = spark.read.parquet(file["path"])
       formatted_cols = [transform_column(f) for f in kpi.schema.fields]
       kpi_transformed = kpi.select(formatted_cols)
       egress_to_csv(kpi_transformed, file["file_name"])
   ```

3. **Save to HDFS and Shared Drive**

   * The `egress_to_csv` function writes the dataframe to CSV locally, then uploads it to the HDFS egress path.
   * Command template:
     `hdfs dfs -put -f <local_path> <hdfs_path>`
   * Verify the file transfer by listing the HDFS directory:

     ```
     !hdfs dfs -ls hdfs:///apps/hive/warehouse/esg/pre-prod/pod/external_disclosures/egress/
     ```

4. **Final Transfer and Automation**

   * Trigger downstream processes or notifications using REST API calls or Powershell scripts if required.
   * Example:

     ```powershell
     Invoke-RestMethod -Uri "<automation_endpoint>" -Method Post -ContentType "application/json" -Body '{"body":{"data":{"projectName":"ESG","feed_name":"ESG_FLD_EUTAX_REGULAR","event":"start"}}}'
     ```

5. **Validation**

   * Check the destination shared folder for the expected egressed output file.
   * Example shared drive location:

     ```
     \\hbeu.adroot.hsbc\UK\HOST\application\EGS_SHARED_NAS\external_disclosures\Automation
     ```

#### Key Notes

* **Automation and Traceability**: All output files are timestamped and egressed via scripts to ensure version control and auditability.
* **Dynamic Handling**: The process is designed to handle multiple files and schema changes by dynamically applying transformations and iterating over source files.
* **Audit Trail**: HDFS and shared drive checks should be part of the egress routine to ensure data integrity and completeness.

---

**This documentation can be added as "Step 2.8" to your slide or process document, matching your format.** If you need a slide draft or further refinement for PowerPoint, let me know your preferred template or previous slide structure.
