# PySpark Reference

## Imports

```python
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window
```

## DataFrame selection and columns

```python
df = df.select("name", "age", F.col("dob").alias("date_of_birth"))
df = df.withColumn("status", F.lit("PASS"))
df = df.withColumnRenamed("dob", "date_of_birth")
df = df.drop("mod_dt", "mod_username")
```

## Filtering and sorting

```python
df = df.filter(F.col("age") > 25)
df = df.filter((F.col("age") > 25) & (F.col("is_adult") == "Y"))
df = df.filter(F.col("first_name").isin(["Bob", "Mike"]))
df = df.orderBy(F.col("age").asc())
```

## Joins

```python
df = df.join(people, on="person_id", how="left")
df = df.join(other, df.id == other.person_id, how="left")
df = df.join(other, on=["first_name", "last_name"], how="inner")
df = df.join(other, on="person_id", how="leftanti")
```

## Null handling and casting

```python
df = df.withColumn("price", F.col("price").cast(T.DoubleType()))
df = df.fillna({"first_name": "Tom", "age": 0})
df = df.withColumn(
    "last_name",
    F.coalesce(F.col("last_name"), F.col("surname"), F.lit("N/A")),
)
df = df.dropDuplicates()
df = df.dropDuplicates(["name", "height"])
```

## Aggregations

```python
df = df.groupBy("country").agg(
    F.count("*").alias("row_count"),
    F.sum("revenue").alias("total_revenue"),
    F.avg("revenue").alias("avg_revenue"),
)
df = df.groupBy("country").count()
```

## String functions

```python
df = df.withColumn("short_id", F.col("id").substr(1, 10))
df = df.withColumn("name", F.trim(F.col("name")))
df = df.withColumn(
    "full_name",
    F.concat_ws(" ", F.col("fname"), F.col("lname")),
)
df = df.withColumn(
    "id",
    F.regexp_replace(F.col("id"), "0F1(.*)", "1F1-$1"),
)
df = df.withColumn(
    "digits",
    F.regexp_extract(F.col("id"), "[0-9]*", 0),
)
```

## Date and timestamp functions

```python
df = df.withColumn("date", F.to_date(F.col("date_str")))
df = df.withColumn("ts", F.to_timestamp(F.col("ts_str"), "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("day_diff", F.datediff(F.col("end_date"), F.col("start_date")))
df = df.withColumn("next_day", F.date_add(F.col("date"), 1))
df = df.withColumn("prev_day", F.date_sub(F.col("date"), 1))
df = df.withColumn("today", F.current_date())
df = df.withColumn("now", F.current_timestamp())
```

## Arrays and structs

```python
df = df.withColumn("arr", F.array(F.col("a"), F.col("b")))
df = df.withColumn("s", F.struct(F.col("a"), F.col("b")))
df = df.withColumn("item", F.explode(F.col("arr")))
df = df.withColumn("arr_size", F.size(F.col("arr")))
df = df.withColumn("has_x", F.array_contains(F.col("arr"), "x"))
```

## Window functions

```python
w = Window.partitionBy("country").orderBy(F.col("event_ts").desc())
df = df.withColumn("rn", F.row_number().over(w))
df = df.withColumn("lag_val", F.lag(F.col("value"), 1).over(w))
df = df.withColumn("rolling_sum", F.sum("value").over(w.rowsBetween(-6, 0)))
```

# Spark SQL Reference

## Query structure

```sql
CREATE TABLE `/path/to/target/dataset` AS
SELECT * FROM `/path/to/source/dataset`
```

No trailing semicolons.

## Comments

```sql
-- Line comment
/* Block comment */
```

## Dataset and column references

- Dataset paths use backticks.
- Dataset and column names are case-sensitive.

```sql
SELECT Name FROM `/path/to/source/dataset`
```

## Derived columns and aliases

- Alias required for expressions in SELECT.
- Aliases not allowed in WHERE or GROUP BY.
- Aliases allowed in ORDER BY and HAVING.

```sql
SELECT Lower(Name) AS lowercase_name
FROM `/path/to/source/dataset`
WHERE Lower(Name) = "sara"

SELECT Lower(Name) AS lowercase_name, Sum(Val) AS total
FROM `/path/to/source/dataset`
GROUP BY Lower(Name)
ORDER BY total
HAVING total > 100
```

## Casting

```sql
CAST(expr AS <TYPE>)
```

Types: boolean, tinyint, smallint, int, bigint, float, double, decimal, date, timestamp, binary, string, variant.

## Date and timestamp parsing

```sql
CAST("2016-07-30" AS DATE)
CAST("2016-07-30" AS TIMESTAMP)
TO_DATE(CAST(UNIX_TIMESTAMP("07302016", "MMddyyyy") AS TIMESTAMP))
CAST("2016-07-30 11:29:27" AS DATE)
TO_DATE("2016-07-30T11:29:27.000+00:00")
TO_DATE("2016-07-30 11:29:27")
```

## Common aggregate functions

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
