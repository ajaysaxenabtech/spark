

```

import os, subprocess, shlex, tempfile, json, textwrap
from pathlib import Path

# -- 1️⃣  minimal env so we don't OOM -----------------
os.environ["SPARK_PRINT_LAUNCH_COMMAND"] = "1"          # <-- makes spark-submit echo full cmd
os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.executor.memory=1g pyspark-shell"

# -- 2️⃣  build the launch cmd exactly like PySpark does -------------
spark_home = os.environ["SPARK_HOME"]
submit = Path(spark_home) / "bin" / "spark-submit"

cmd = f"{submit} --version"           # 'spark-submit --version' is enough to spin a JVM

print("### Running -->", cmd, "\n")
proc = subprocess.run(
    shlex.split(cmd),
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
)
print(proc.stdout)

