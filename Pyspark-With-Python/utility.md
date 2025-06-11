

```


# ---------- GCDU_OLD ----------
f1 = gcdu_old.where(gcdu_old["group_system_id"] != "HGHQ")\
             .drop("mastergroup_id", "mastergroup_name",
                   "nace_code", "nace_code_desc")

f2 = gcdu_old.select("gid", "mastergroup_id", "mastergroup_name",
                     "nace_code", "nace_code_desc")\
             .filter("mastergroup_id is not null")\
             .filter(gcdu_old["group_system_id"] == "HGHQ")

gcdu_old = f1.join(f2, ["gid"], how="left")

base_us    = gcdu_old.where(gcdu_old["group_system_id"] == "CDUUS")\
                     .withColumn("cin",
                                 F.regexp_replace("cin", r"^\[USCIF\]", ""))\
                     .withColumn("cin",
                                 F.regexp_replace("cin", r"^[0]*", ""))

base_other = gcdu_old.where(gcdu_old["group_system_id"] != "CDUUS")

gcdu_old   = base_us.unionByName(base_other)

# ---------- GCDU_NEW ----------
f1 = gcdu_new.where(gcdu_new["group_system_id"] != "HGHQ")\
             .drop("mastergroup_id", "mastergroup_name",
                   "nace_code", "nace_code_desc")

f2 = gcdu_new.select("gid", "mastergroup_id", "mastergroup_name",
                     "nace_code", "nace_code_desc")\
             .filter("mastergroup_id is not null")\
             .filter(gcdu_new["group_system_id"] == "HGHQ")

gcdu_new = f1.join(f2, ["gid"], how="left")

base_us    = gcdu_new.where(gcdu_new["group_system_id"] == "CDUUS")\
                     .withColumn("cin",
                                 F.regexp_replace("cin", r"^\[USCIF\]", ""))\
                     .withColumn("cin",
                                 F.regexp_replace("cin", r"^[0]*", ""))

base_other = gcdu_new.where(gcdu_new["group_system_id"] != "CDUUS")

gcdu_new   = base_us.unionByName(base_other)

# ---------- GCDU_OLDER ----------
f1 = gcdu_older.where(gcdu_older["group_system_id"] != "HGHQ")\
               .drop("mastergroup_id", "mastergroup_name",
                     "nace_code", "nace_code_desc")

f2 = gcdu_older.select("gid", "mastergroup_id", "mastergroup_name",
                       "nace_code", "nace_code_desc")\
               .filter("mastergroup_id is not null")\
               .filter(gcdu_older["group_system_id"] == "HGHQ")

gcdu_older = f1.join(f2, ["gid"], how="left")

base_us    = gcdu_older.where(gcdu_older["group_system_id"] == "CDUUS")\
                       .withColumn("cin",
                                   F.regexp_replace("cin", r"^\[USCIF\]", ""))\
                       .withColumn("cin",
                                   F.regexp_replace("cin", r"^[0]*", ""))

base_other = gcdu_older.where(gcdu_older["group_system_id"] != "CDUUS")

gcdu_older = base_us.unionByName(base_other)

# ---------- Quick sanity checks ----------
gcdu_older.where("cin = 1064300936")\
          .select("cin", "gid", "mastergroup_id").toPandas()

gcdu_old.where("cin = 1064300936")\
        .select("cin", "gid", "mastergroup_id").toPandas()

gcdu_new.where("cin = 1064300936")\
        .select("cin", "gid", "mastergroup_id").toPandas()
