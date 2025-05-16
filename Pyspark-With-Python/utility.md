

---
```python

import subprocess

ps_command = 'Get-Process | Sort-Object CPU -Descending | Select-Object -First 5'

result = subprocess.run(
    ["powershell", "-Command", ps_command],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True
)

print(result.stdout)


```