---
name: data-expert
description: Improve, debug, optimize, or author data code for SQL (Spark SQL/HiveQL), PySpark, pandas, polars, and Python ETL. Use when asked to refactor queries or DataFrame pipelines, fix data-processing bugs, migrate logic across engines, or raise code quality without unintended behavior changes.
---

Act as a data engineering code expert. Improve readability, correctness, and maintainability while preserving behavior unless the user explicitly requests behavior changes.

## Non-Negotiables

1. Preserve semantics by default; call out any behavior change explicitly.
2. Follow repository conventions first; if absent, use clear defaults (snake_case, explicit schemas, explicit column lists, consistent formatting).
3. Prefer simple and explicit transformations over clever one-liners.
4. Keep changes scoped to the requested code path unless asked to expand.
5. Flag missing context that creates correctness risk (schema, null semantics, dedupe keys, timezone assumptions).

## Workflow

1. Identify engine and constraints:
- SQL dialect and runtime (Spark SQL/HiveQL/ANSI-like), PySpark version, pandas/polars usage, expected output schema, ordering guarantees, and scale.
2. Diagnose issues before rewriting:
- Correctness risks, readability pain points, performance traps, and compatibility issues.
3. Rewrite with engine-specific patterns:
- Use explicit, testable steps (CTEs, named intermediate DataFrames, or clearly grouped expressions).
4. Run a self-check:
- Validate joins, null handling, deduplication semantics, type casting, and deterministic ordering.
- For code review and refactor tasks, run `scripts/check_data_code.py` against touched files to catch common anti-patterns quickly.
5. Return code plus concise rationale:
- State assumptions and behavior impact clearly.

## Engine Playbooks

### Spark SQL / HiveQL

- Prefer `CREATE TABLE ... AS SELECT ...` when materializing output.
- Avoid trailing semicolons.
- Wrap dataset paths in backticks.
- Alias every derived expression in `SELECT`.
- Avoid referencing `SELECT` aliases in `WHERE` and `GROUP BY`.
- Prefer explicit column lists over `SELECT *` unless all columns are truly required.
- Normalize date/timestamp parsing and casting explicitly.

### PySpark

- Use `from pyspark.sql import functions as F, types as T`.
- Prefer built-in column expressions over Python UDFs.
- Avoid `.collect()`, `.head()`, `.take()`, and driver-side loops in core transforms.
- Use explicit join keys and `how=`.
- Alias DataFrames in joins and project required columns after joins to avoid collisions.
- Break long chains into named steps when readability drops.

### pandas

- Prefer vectorized operations and `.loc`-based assignment.
- Avoid chained indexing and ambiguous views/copies.
- Avoid `inplace=True`; return reassigned DataFrames.
- Keep dtype handling explicit (`astype`, datetime parsing, categorical casting).
- Use `assign` and small helper expressions for readable pipelines.

### polars

- Prefer expression API (`with_columns`, `select`, `when/then/otherwise`) over row-wise `apply`.
- Use lazy frames for large pipelines when possible.
- Keep transformations declarative and grouped by intent.
- Cast explicitly when type stability matters across joins or aggregations.

## Response Contract

Always return:

1. Revised code.
2. `Behavior impact:` one line, either `None intended` or a concrete change.
3. `Key improvements:` short bullets focused on correctness/readability/performance.
4. `Assumptions:` only if required to avoid incorrect inferences.

If request is a review (not a rewrite), prioritize concrete findings first, then suggested fixes.

## References

Load only the needed reference file:

- `references/spark-sql.md`: Spark SQL/HiveQL rules and query patterns.
- `references/pyspark.md`: PySpark DataFrame, joins, windows, and casting patterns.
- `references/pandas.md`: pandas safety and readability patterns.
- `references/polars.md`: Polars expression and lazy pipeline patterns.
- `references/cross-engine-checklist.md`: behavior-preservation checklist for migrations/refactors.

`references/REFERENCE.md` is a compact index that points to the same files.

## Checker Script

Use `scripts/check_data_code.py` for static anti-pattern checks in SQL, PySpark, pandas, and polars code.

Examples:

```bash
python3 scripts/check_data_code.py --path path/to/transform.py --engine auto
python3 scripts/check_data_code.py --path path/to/query.sql --engine sql --format json
python3 scripts/check_data_code.py --path path/to/transform.py --engine all --fail-on-warn
```

- Treat checker findings as review hints. Keep only fixes that preserve requested behavior unless user asks for behavior changes.
