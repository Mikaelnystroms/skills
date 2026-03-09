---
name: pyspark-style-guide
description: Write, review, refactor, or debug PySpark DataFrame code using a readability-first style guide. Use when working on PySpark transforms, joins, windows, schema reshaping, or style-focused reviews where readability, maintainability, and behavior preservation matter.
---

Act as a PySpark specialist who applies a strict readability-first style guide while preserving behavior unless the user explicitly requests a behavior change.

## Non-Negotiables

1. Preserve semantics by default. Call out any change to output schema, null handling, join cardinality, ordering, window behavior, or fallback logic.
2. Follow repository conventions first. Use the bundled style guide when the repo does not establish a stronger local pattern.
3. Prefer built-in expressions, explicit joins, and readable transformation steps over compact but opaque chains.
4. Keep schema-shaping obvious. Use `select(...)` when defining the outgoing schema and `withColumn(...)` for focused enrichment or replacement.
5. Avoid style-only rewrites that increase risk without making the transform materially clearer.

## Workflow

1. Identify the constraints before editing:
- Input and output schema
- Join keys and expected cardinality
- Null and timezone semantics
- Ordering guarantees
- Window partition, order, and frame requirements

2. Load `references/pyspark-style-guide.md` before rewriting or reviewing non-trivial PySpark code.

3. Rewrite around the guide's main patterns:
- Prefer `"column_name"` or `F.col("column_name")` for normal references. Use DataFrame aliases only when disambiguating joins.
- Extract complex predicates, derived expressions, and repeated literals into named variables before `select(...)` or `withColumn(...)`.
- Keep `select(...)` lists shallow and readable. Precompute multi-branch logic before final projection.
- Use `F.lit(...)` for literals and `F.lit(None).cast(...)` for typed nulls.
- Make joins explicit with aliases, keys, and `how=`.
- Define windows intentionally and specify frames when semantics depend on them.
- Wrap chains in parentheses instead of using backslash continuations.

4. Run `scripts/check_pyspark_style.py` on touched PySpark files for review and refactor tasks. Treat it as a heuristic backstop, not an authoritative formatter.

5. Return:
- Revised code
- `Behavior impact:` with `None intended` or a concrete change
- `Key improvements:` focused on readability, correctness, and maintainability
- `Assumptions:` only when missing context affects correctness

## Review Checklist

- Are joins explicit about keys and `how=`?
- Are window specs named and intentionally framed?
- Does `select(...)` clearly describe the output schema?
- Are complex expressions lifted into named variables instead of buried in long chains?
- Are literals and business-rule thresholds named when reused or non-obvious?
- Do comments explain rationale instead of repeating the API call?
- Does any `otherwise(...)` branch hide unexpected values that should stay visible?

## References

Load only what is needed:

- `references/pyspark-style-guide.md`: distilled rules and review prompts for DataFrame code

## Checker Script

Use `scripts/check_pyspark_style.py` for fast, heuristic review of common PySpark style issues.

Examples:

```bash
python3 scripts/check_pyspark_style.py --path path/to/job.py
python3 scripts/check_pyspark_style.py --path path/to/job.py --format json
python3 scripts/check_pyspark_style.py --path path/to/job.py --fail-on-warn
```

Treat findings as prompts to inspect the code, not automatic defects.
