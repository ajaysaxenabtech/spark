

---
```
git filter-branch --force --index-filter \
"git rm --cached --ignore-unmatch archive/RLD1/eu_taxonomy/fcd/DESG-17355_fcd/dwf_logreport.log" \
--prune-empty --tag-name-filter cat -- --all


---



```