# Polars Reference

## Expression-First Transform Pattern

```python
import polars as pl

out = (
    df.with_columns(
        [
            pl.col("order_date").str.strptime(pl.Date, strict=False),
            pl.col("amount").cast(pl.Float64),
            pl.when(pl.col("amount").is_null())
            .then(0.0)
            .otherwise(pl.col("amount"))
            .alias("amount_clean"),
        ]
    )
    .group_by(["country", "channel"])
    .agg(
        [
            pl.col("order_id").n_unique().alias("orders"),
            pl.col("amount_clean").sum().alias("revenue"),
        ]
    )
)
```

## Join and Cleanup

```python
out = (
    left.join(
        right.select(["user_id", "segment"]),
        on="user_id",
        how="left",
    )
    .rename({"segment": "user_segment"})
)
```

## Lazy Pipeline Pattern

```python
out = (
    pl.scan_parquet("events.parquet")
    .filter(pl.col("event_dt") >= pl.lit("2025-01-01"))
    .group_by("user_id")
    .agg(pl.len().alias("event_count"))
    .collect()
)
```

## Avoid

- Row-wise `.apply` where expressions are available.
- Implicit type coercions in joins and aggregations.
- Eager execution for large pipelines when lazy execution is available.
