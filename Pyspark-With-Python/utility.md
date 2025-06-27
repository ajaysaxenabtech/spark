

```

import os, subprocess, shlex
from pathlib import Path

# 1️⃣  make spark-submit print the full JVM launch command
os.environ["SPARK_PRINT_LAUNCH_COMMAND"] = "1"
# tiny JVM so we don’t blow the cgroup
os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.executor.memory=1g pyspark-shell"

spark_home = os.environ["SPARK_HOME"]
submit     = Path(spark_home) / "bin" / "spark-submit"
cmd        = f"{submit} --version"

print("### Running -->", cmd, "\n")

proc = subprocess.run(
    shlex.split(cmd),
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    universal_newlines=True   # ✔ works on Py 3.6
)
print(proc.stdout)

