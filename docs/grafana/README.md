# Queries for panels in grafana dashboard

## ETL Step Duration
```sql
SELECT 
    timestamp as time,
    JSONExtractString(details, 'step') as metric,
    value as duration_seconds
FROM pipeline_metrics
WHERE category = 'elt'
  AND name = 'step_duration'
  AND $__timeFilter(timestamp)
ORDER BY time
```

## Data Quality Issues
```sql
SELECT 
    formatDateTime(timestamp, '%Y-%m-%d %H:%i') as time,
    name AS issue,
    JSONExtractString(details, 'column') as column,
    value as count
FROM pipeline_metrics
WHERE category = 'data_quality'
  AND value > 0
  AND timestamp > now() - INTERVAL 7 DAY
ORDER BY timestamp DESC
LIMIT 10
```

## Rows Processed
```sql
SELECT 
    SUM(value) AS value
FROM pipeline_metrics
WHERE category = 'elt'
  AND name = 'rows_processed'
```
