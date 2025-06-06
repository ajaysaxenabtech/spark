

```

from pyspark.sql import functions as F

df = df.withColumn(
    "aentdis_check",
    F.when(
        # CASE 1: GRCA_description contains 'ICP'
        F.col("GRCA_description").rlike("(?i)ICP"),
        # aentdis must not be blank, must be in saracen_list
        (F.trim(F.col("aentdis")).isNotNull()) &
        (F.trim(F.col("aentdis")) != "") &
        (F.col("aentdis").isin(saracen_list))
    )
    .when(
        # CASE 2: GRCA_description does NOT contain 'ICP'
        ~F.col("GRCA_description").rlike("(?i)ICP"),
        (
            # If aentdis is blank: valid
            (F.trim(F.col("aentdis")).isNull()) | (F.trim(F.col("aentdis")) == "")
            # If aentdis is not blank: must be in saracen_list
            | (F.col("aentdis").isin(saracen_list))
        )
    )
    .otherwise(False)
)
