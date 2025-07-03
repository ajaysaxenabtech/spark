


### re Module
---
**re**

```python

import re

data = ['abc', 'ICP100', 'xyz', 'icp-certified', 'something']

# Regex pattern for case-insensitive match
pattern = re.compile(r'(?i)icp')

# Filter list
matches = [item for item in data if pattern.search(item)]

print(matches)

```

---