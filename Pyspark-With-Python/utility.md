

---
```python

.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
.config("spark.kryoserializer.buffer", "64m") \
.config("spark.kryoserializer.buffer.max", "512m") \

```
---

```cmd
git checkout 1e24c6203d472e5dd62757a78e38f257404d45d4 -- .

```