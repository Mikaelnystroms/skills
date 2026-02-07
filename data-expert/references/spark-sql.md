# Spark SQL / HiveQL Reference

## Core Rules

- Use `CREATE TABLE ... AS SELECT ...` for materialization unless project patterns specify otherwise.
- Avoid trailing semicolons.
- Wrap dataset paths in backticks.
- Alias every derived expression in `SELECT`.
- Do not reference `SELECT` aliases in `WHERE` and `GROUP BY`.
- Allow aliases in `ORDER BY` and `HAVING`.
- Prefer explicit column lists over `SELECT *` unless intentionally selecting every column.

## Query Skeleton

```sql
CREATE TABLE `/path/to/target/dataset` AS
WITH staged AS (
    SELECT
        id,
        CAST(event_ts AS TIMESTAMP) AS event_ts,
        CAST(amount AS DOUBLE) AS amount
    FROM `/path/to/source/dataset`
),
aggregated AS (
    SELECT
        id,
        MAX(event_ts) AS latest_event_ts,
        SUM(amount) AS total_amount
    FROM staged
    GROUP BY id
)
SELECT
    id,
    latest_event_ts,
    total_amount
FROM aggregated
```

## Casting and Date Parsing

```sql
CAST(expr AS DOUBLE)
CAST("2016-07-30" AS DATE)
CAST("2016-07-30 11:29:27" AS TIMESTAMP)
TO_DATE(CAST(UNIX_TIMESTAMP("07302016", "MMddyyyy") AS TIMESTAMP))
```

## Common Aggregates

- APPROX_COUNT_DISTINCT
- AVG
- COLLECT_LIST
- COLLECT_SET
- COUNT
- MAX
- MIN
- SUM
- STDDEV_POP
- STDDEV_SAMP
- VAR_POP
- VAR_SAMP

## Quick Review Checks

- Confirm join keys and join type are explicit.
- Confirm deduplication intent is explicit.
- Confirm ordering is explicit where determinism matters.
- Confirm behavior with nulls in filter and aggregate clauses.
