

---
```python

import subprocess

cmd = "ps aux --sort=-%cpu | head -n 6"

result = subprocess.run(
    cmd,
    shell=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    universal_newlines=True
)

print(result.stdout)


```