

```

F.when(
    F.col('date_column').isNotNull(),
    F.to_date(F.col('date_column'), 'yyyy-MM-dd') <= F.to_date(F.col('reporting_date'), 'yyyy-MM-dd')
)

