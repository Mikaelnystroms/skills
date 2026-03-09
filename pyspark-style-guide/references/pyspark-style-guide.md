# PySpark Style Guide

Use this reference when writing, reviewing, or refactoring PySpark DataFrame code. It is a condensed style guide for readability-first DataFrame code.

## Core Intent

- Optimize for transforms that a reviewer can scan top to bottom.
- Make schema changes, join semantics, and window behavior obvious.
- Prefer explicit intermediate intent over squeezing everything into one chain.

## Columns And Expressions

- Prefer simple string column references or `F.col("name")` for routine access.
- Use qualified columns from aliased DataFrames only when resolving join ambiguity.
- Extract non-trivial `Column` expressions into named variables before final projection.
- Use `F.lit(...)` for literals.
- Use `F.lit(None).cast(...)` when a typed null is required.

Example:

```python
is_recent = F.col("event_date") >= F.date_sub(F.current_date(), 30)
normalized_status = F.when(F.col("status").isin("new", "pending"), F.lit("open"))
```

## Shape The Output Deliberately

- Use `select(...)` when defining the outgoing schema, renaming many columns, or making the final output obvious.
- Use `withColumn(...)` for focused additions or replacements.
- Keep `select(...)` expressions shallow. Precompute complex logic before the final `select(...)`.
- Prefer a short sequence of named DataFrames when a single chain becomes hard to scan.

## Join Style

- Alias both sides when column names overlap or when the join predicate is not trivial.
- Pass `how=` explicitly instead of relying on Spark defaults.
- Project required columns after the join to remove ambiguity and accidental carry-through.
- Lift compound join predicates into named variables when they stop fitting on one line.

## Window Style

- Name non-trivial `WindowSpec` values.
- Specify partitioning and ordering intentionally.
- Add `rowsBetween(...)` or `rangeBetween(...)` when the frame affects semantics. Do not rely on defaults when readers need to reason about the result.
- Keep window calculations grouped in one readable step.

## Conditionals And Fallbacks

- Keep `when(...).when(...).otherwise(...)` branches vertically aligned and easy to scan.
- Review broad `otherwise(...)` branches carefully. Unexpected values should often remain visible rather than being silently folded into a default bucket.
- Name reusable thresholds and business-rule constants instead of repeating magic values.

## Formatting

- Wrap multiline chains in parentheses instead of using backslashes.
- Group steps by intent: filter, enrich, aggregate, final projection.
- Break code into named intermediates when a chain mixes too many concepts.
- Comment on business rationale or surprising semantics, not on obvious API calls.

## Review Prompts

- Can a reviewer identify the output schema quickly?
- Are join keys, join type, and duplicate-column handling obvious?
- Are window boundaries explicit enough to reason about edge cases?
- Are complex expressions named before they are used?
- Does the code avoid Python UDFs and driver-side actions unless there is a real need?
