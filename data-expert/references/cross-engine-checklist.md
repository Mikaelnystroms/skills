# Cross-Engine Checklist

Use this checklist for refactors, engine migrations, and major cleanups.

## Semantics

- Confirm row grain before and after each join.
- Confirm filter behavior with nulls.
- Confirm dedupe key and tie-break ordering.
- Confirm aggregate grain and grouping keys.

## Types and Time

- Confirm numeric cast behavior (precision/overflow/nullable rules).
- Confirm timestamp parsing and timezone assumptions.
- Confirm string normalization assumptions (trim/case/encoding).

## Determinism

- Confirm output ordering where required by downstream systems.
- Confirm window ordering columns are complete and stable.

## Operational Concerns

- Confirm no accidental driver collection for distributed engines.
- Confirm join strategy and projected columns are scale-appropriate.
- Confirm behavior changes are explicitly called out in final notes.
