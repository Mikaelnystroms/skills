# PySpark Reference

## Imports

```python
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window
```

## Selection and Columns

```python
df = df.select("name", "age", F.col("dob").alias("date_of_birth"))
df = df.withColumn("status", F.lit("PASS"))
df = df.withColumnRenamed("dob", "date_of_birth")
df = df.drop("mod_dt", "mod_username")
```

## Filtering and Sorting

```python
df = df.filter(F.col("age") > 25)
df = df.filter((F.col("age") > 25) & (F.col("is_adult") == "Y"))
df = df.filter(F.col("first_name").isin(["Bob", "Mike"]))
df = df.orderBy(F.col("age").asc())
```

## Joins

```python
base = left.alias("l")
dim = right.alias("r")

joined = (
    base.join(dim, on=F.col("l.user_id") == F.col("r.user_id"), how="left")
    .select(
        F.col("l.user_id"),
        F.col("l.event_ts"),
        F.col("r.segment").alias("user_segment"),
    )
)
```

## Nulls, Casting, and Dedupe

```python
df = df.withColumn("price", F.col("price").cast(T.DoubleType()))
df = df.fillna({"first_name": "Tom", "age": 0})
df = df.withColumn(
    "last_name",
    F.coalesce(F.col("last_name"), F.col("surname"), F.lit("N/A")),
)
df = df.dropDuplicates(["name", "height"])
```

## Aggregations

```python
agg = df.groupBy("country").agg(
    F.count("*").alias("row_count"),
    F.sum("revenue").alias("total_revenue"),
    F.avg("revenue").alias("avg_revenue"),
)
```

## Window Functions

```python
w = Window.partitionBy("country").orderBy(F.col("event_ts").desc())
df = df.withColumn("rn", F.row_number().over(w))
df = df.withColumn("lag_val", F.lag(F.col("value"), 1).over(w))
df = df.withColumn("rolling_sum", F.sum("value").over(w.rowsBetween(-6, 0)))
```

## Avoid

- Python UDFs where built-ins exist.
- Driver actions (`collect`, `take`, `head`) in core transforms.
- Ambiguous joins without explicit keys or `how=`.
