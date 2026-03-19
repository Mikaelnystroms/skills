---
name: code-simplifier
description: Simplifies code using the KISS principle for clarity, consistency, and maintainability while preserving exact behavior. Defaults to recently changed files unless the user asks for broader scope.
---

# Code Simplifier

Use this skill when the user asks to simplify, clean up, or refactor code without changing behavior.

It is designed to work across languages and tools. It should be useful for Python, PySpark, SQL, JavaScript, TypeScript, shell scripts, and other code the user writes, while adapting to the context of the code instead of assuming a single editor, model, vendor, or stack.

Apply the KISS principle throughout: choose the simplest clear solution that fully preserves behavior, and avoid adding abstraction, indirection, or structure that the code does not need.

## Goals

1. Preserve functionality exactly.
2. Improve readability, consistency, and maintainability.
3. Respect the code's context, constraints, and intended style.
4. Prefer explicit, easy-to-follow code over clever or overly compact rewrites.
5. Favor the simplest implementation that is clear, sufficient, and easy to maintain.

## Scope

- Default to code touched in the current task/session.
- Expand scope only when the user explicitly requests broader refactoring.

## Guardrails

- Do not change externally observable behavior.
- Do not change public interfaces, schemas, file formats, or config contracts unless asked.
- Prefer explicit and readable code over compact or clever code.
- Prefer fewer moving parts, less indirection, and straightforward control flow when they preserve clarity and behavior.
- Do not introduce extra helpers, layers, patterns, or configuration unless they clearly reduce complexity in context.
- Avoid broad formatting-only churn unrelated to simplification.
- Keep comments only when they add real context.
- Preserve error-handling intent; do not weaken safety checks.
- Avoid imposing stack-specific rules unless the code clearly depends on them.

## Conventions

- Follow clear, context-appropriate conventions already present in the code when they help consistency.
- Do not enforce vendor-, editor-, or model-specific conventions.
- Do not impose language-specific preferences unless they are already established, explicitly requested, or clearly improve clarity without changing behavior.
- When project standards are unclear, choose the most readable and maintainable option instead of the shortest one.
- When multiple behavior-preserving options are equally valid, choose the simpler one.

## Language-Specific Guidance

- For Python and PySpark, prefer clear step-by-step transformations, descriptive names, small coherent helpers, and explicit handling of nulls, types, and branching.
- For SQL, favor readable query structure, stable semantics, clear aliases, and simplifications that do not change join behavior, filter timing, grouping, ordering, or null handling.
- For typed languages, preserve existing type contracts and improve type clarity where it makes code easier to understand.
- For frontend or framework-heavy code, follow the established component, module, and error-handling patterns only when they are relevant to the code being simplified.

## Simplification Workflow

1. Identify candidate sections such as duplication, deep nesting, unclear naming, or noisy logic.
2. Apply small, behavior-preserving refactors that reduce complexity rather than redistribute it.
3. Re-check for accidental API, contract, or semantic changes.
4. Run the smallest relevant validation available, such as tests, lint, type-check, or query verification, when possible.
5. Report what changed and what was intentionally left unchanged.

## Common Refactors

- Replace deep nesting with guard clauses.
- Extract coherent helper functions.
- Remove redundant variables or branches.
- Consolidate repeated logic.
- Clarify naming for variables and functions.
- Replace magic literals with named constants when helpful.
- Simplify conditionals and avoid nested ternaries for multi-branch logic.
- Flatten control flow when it improves readability.
- Remove unnecessary abstraction or indirection when inline logic is clearer.
- Tighten type or null handling where it improves clarity and safety.
- Break apart overly dense expressions when intermediate names improve understanding.

## Output Expectations

- Keep summaries concise and concrete.
- List significant changes that affect readability or maintenance.
- Call out validation run, or explain why validation was not run.
