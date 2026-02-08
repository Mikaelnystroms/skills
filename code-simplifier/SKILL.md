---
name: code-simplifier
description: Simplifies and refines code for clarity, consistency, and maintainability while preserving functionality. Defaults to recently changed files unless the user asks for broader scope.
---

# Code Simplifier

Use this skill when the user asks to simplify, clean up, or refactor code without changing behavior.

## Goals

1. Preserve functionality exactly.
2. Improve readability and maintainability.
3. Align with repository conventions already in use.

## Scope

- Default to code touched in the current task/session.
- Expand scope only when the user explicitly requests broader refactoring.

## Guardrails

- Do not change externally observable behavior.
- Do not change public interfaces, schemas, file formats, or config contracts unless asked.
- Prefer explicit and readable code over compact/clever code.
- Avoid broad formatting-only churn unrelated to simplification.
- Keep comments only when they add real context.
- Preserve error-handling intent; do not weaken safety checks.

## Conventions

- Follow local project docs and patterns in the target files.
- Keep style/framework choices consistent with the existing codebase.
- Do not enforce stack-specific preferences (e.g., React/ESM/function-style rules) unless the repository clearly requires them.

## Simplification Workflow

1. Identify candidate sections (duplication, deep nesting, unclear naming, noisy logic).
2. Apply small, behavior-preserving refactors.
3. Re-check for accidental API/contract changes.
4. Run the smallest relevant validation (tests/lint/type-check) when possible.
5. Report what changed and what was intentionally left unchanged.

## Common Refactors

- Replace deep nesting with guard clauses.
- Extract coherent helper functions.
- Remove redundant variables/branches.
- Consolidate repeated logic.
- Clarify naming for variables/functions.
- Replace magic literals with named constants (when helpful).
- Simplify conditionals (avoid nested ternaries for multi-branch logic).
- Tighten type/null handling where it improves clarity and safety.

## Output Expectations

- Keep summaries concise and concrete.
- List significant changes that affect readability/maintenance.
- Call out validation run (or why it was not run).
