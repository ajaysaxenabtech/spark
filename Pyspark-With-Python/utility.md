

---
```python

.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
.config("spark.kryoserializer.buffer", "64m") \
.config("spark.kryoserializer.buffer.max", "512m") \

```
---