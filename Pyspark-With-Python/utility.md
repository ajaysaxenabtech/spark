

---
```python

.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
.config("spark.kryoserializer.buffer", "64m") \
.config("spark.kryoserializer.buffer.max", "512m") \

```
---

```cmd
echo "egress_data/*.csv" > .gitignore
git add .
git commit -m "Clean history, removed large files"


```