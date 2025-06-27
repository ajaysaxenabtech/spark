

```

# 1. Most Linux distros
!which java

# 2. Try readlink if the above prints something
!readlink -f $(which java)

# 3. Common Cloudera / RHEL locations
!ls -d /usr/java/* 2>/dev/null
!ls -d /usr/lib/jvm/* 2>/dev/null

