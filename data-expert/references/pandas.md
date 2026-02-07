# pandas Reference

## Safe Assignment

```python
df = df.copy()
df.loc[df["amount"].isna(), "amount"] = 0
df = df.assign(
    order_date=pd.to_datetime(df["order_date"], errors="coerce"),
    amount=df["amount"].astype("float64"),
)
```

## Groupby with Named Outputs

```python
agg = (
    df.groupby(["country", "channel"], as_index=False)
    .agg(
        orders=("order_id", "nunique"),
        revenue=("amount", "sum"),
    )
)
```

## Merge with Validation

```python
joined = (
    left.merge(
        right[["user_id", "segment"]],
        on="user_id",
        how="left",
        validate="m:1",
    )
    .rename(columns={"segment": "user_segment"})
)
```

## Deterministic Dedupe

```python
latest = (
    df.sort_values(["customer_id", "event_ts"])
    .drop_duplicates(subset=["customer_id"], keep="last")
)
```

## Avoid

- `inplace=True` mutations.
- Chained indexing (for example `df[df["x"] > 0]["y"] = 1`).
- Row-wise `apply` when vectorized expressions are available.
