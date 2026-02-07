#!/usr/bin/env python3
"""Static checker for common data-code anti-patterns.

This script is intentionally lightweight and heuristic-based. It helps catch
high-signal issues quickly during refactors and reviews.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

ALL_ENGINES = {"sql", "pyspark", "pandas", "polars"}


@dataclass(frozen=True)
class Rule:
    rule_id: str
    severity: str
    message: str
    pattern: str
    engines: tuple[str, ...]
    flags: int = 0


@dataclass(frozen=True)
class Finding:
    rule_id: str
    severity: str
    message: str
    line: int
    column: int
    snippet: str


RULES = (
    Rule(
        "DE_SQL_SELECT_STAR",
        "warn",
        "Avoid SELECT * unless full-column projection is required.",
        r"\bSELECT\s+\*",
        ("sql",),
        re.IGNORECASE,
    ),
    Rule(
        "DE_SQL_TRAILING_SEMICOLON",
        "warn",
        "Avoid trailing semicolons in Spark SQL/HiveQL style.",
        r";\s*$",
        ("sql",),
        re.MULTILINE,
    ),
    Rule(
        "DE_SQL_RIGHT_JOIN",
        "warn",
        "Avoid RIGHT JOIN unless there is a clear reason.",
        r"\bRIGHT\s+JOIN\b",
        ("sql",),
        re.IGNORECASE,
    ),
    Rule(
        "DE_PYSPARK_DRIVER_ACTION",
        "warn",
        "Avoid driver actions (collect/head/take) in core transforms.",
        r"\.(collect|head|take)\s*\(",
        ("pyspark",),
    ),
    Rule(
        "DE_PYSPARK_UDF",
        "warn",
        "Prefer built-in expressions over Python UDFs.",
        r"(@udf\b|\b(?:F\.)?udf\s*\()",
        ("pyspark",),
        re.IGNORECASE,
    ),
    Rule(
        "DE_PYSPARK_BACKSLASH_CONTINUATION",
        "warn",
        "Avoid backslash-based line continuation; prefer parentheses.",
        r"\\\s*$",
        ("pyspark",),
        re.MULTILINE,
    ),
    Rule(
        "DE_PANDAS_INPLACE",
        "warn",
        "Avoid inplace=True to keep dataflow explicit.",
        r"\binplace\s*=\s*True\b",
        ("pandas",),
    ),
    Rule(
        "DE_PANDAS_CHAINED_INDEXING",
        "warn",
        "Avoid chained indexing; use .loc for deterministic writes.",
        r"\[[^\]\n]+\]\[[^\]\n]+\]",
        ("pandas",),
    ),
    Rule(
        "DE_PANDAS_APPLY",
        "warn",
        "Review .apply usage; prefer vectorized operations when possible.",
        r"\.apply\s*\(",
        ("pandas",),
    ),
    Rule(
        "DE_POLARS_APPLY",
        "warn",
        "Prefer expression API over row-wise .apply/map_rows.",
        r"\.(apply|map_rows)\s*\(",
        ("polars",),
    ),
    Rule(
        "DE_POLARS_EAGER_READ",
        "warn",
        "For large workloads, prefer scan_* over read_* when feasible.",
        r"\bpl\.read_(csv|parquet|json)\s*\(",
        ("polars",),
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check data code for common anti-patterns.")
    parser.add_argument("--path", help="Path to file to check. If omitted, read from stdin.")
    parser.add_argument(
        "--engine",
        default="auto",
        choices=["auto", "all", "sql", "pyspark", "pandas", "polars"],
        help="Engine to check.",
    )
    parser.add_argument(
        "--format",
        default="text",
        choices=["text", "json"],
        help="Output format.",
    )
    parser.add_argument(
        "--fail-on-warn",
        action="store_true",
        help="Exit with code 1 when warnings are present.",
    )
    parser.add_argument(
        "--max-findings",
        type=int,
        default=200,
        help="Maximum number of findings to print.",
    )
    return parser.parse_args()


def detect_engines(text: str, path: str | None) -> set[str]:
    detected: set[str] = set()
    suffix = Path(path).suffix.lower() if path else ""
    is_python = suffix == ".py" or bool(
        re.search(r"^\s*(from|import|def|class)\b", text, flags=re.MULTILINE)
    )

    if suffix in {".sql", ".hql"}:
        return {"sql"}

    if re.search(r"\bfrom\s+pyspark\.sql\b|\bSparkSession\b|\bF\.|\bwithColumn\b|\bgroupBy\b", text):
        detected.add("pyspark")
    if re.search(r"\bimport\s+pandas\b|\bimport\s+pandas\s+as\s+pd\b|\bpd\.|\.groupby\s*\(|\.merge\s*\(", text):
        detected.add("pandas")
    if re.search(r"\bimport\s+polars\b|\bimport\s+polars\s+as\s+pl\b|\bpl\.|\.with_columns\s*\(|\.group_by\s*\(", text):
        detected.add("polars")
    # Avoid classifying normal Python files as SQL due to SQL-like text in comments/strings.
    if not is_python and re.search(r"\bSELECT\b.*\bFROM\b", text, re.IGNORECASE | re.DOTALL):
        detected.add("sql")
    elif re.search(r"\bspark\.sql\s*\(", text):
        detected.add("sql")

    if not detected:
        if is_python:
            return set()
        return set(ALL_ENGINES)
    return detected


def index_to_line_col(text: str, index: int) -> tuple[int, int]:
    line = text.count("\n", 0, index) + 1
    last_nl = text.rfind("\n", 0, index)
    column = index + 1 if last_nl == -1 else index - last_nl
    return line, column


def line_at(text: str, line_number: int) -> str:
    lines = text.splitlines()
    if 1 <= line_number <= len(lines):
        return lines[line_number - 1].strip()
    return ""


def run_regex_rules(text: str, active_engines: set[str]) -> list[Finding]:
    findings: list[Finding] = []
    for rule in RULES:
        if not set(rule.engines).intersection(active_engines):
            continue
        for match in re.finditer(rule.pattern, text, flags=rule.flags):
            line, column = index_to_line_col(text, match.start())
            findings.append(
                Finding(
                    rule_id=rule.rule_id,
                    severity=rule.severity,
                    message=rule.message,
                    line=line,
                    column=column,
                    snippet=line_at(text, line),
                )
            )
    return findings


def run_custom_pyspark_checks(text: str, active_engines: set[str]) -> list[Finding]:
    if "pyspark" not in active_engines:
        return []

    findings: list[Finding] = []
    for match in re.finditer(r"\.join\s*\((.*?)\)", text, flags=re.DOTALL):
        args = match.group(1)
        if "how=" in args:
            continue
        line, column = index_to_line_col(text, match.start())
        findings.append(
            Finding(
                rule_id="DE_PYSPARK_JOIN_WITHOUT_HOW",
                severity="warn",
                message="Specify how= explicitly in DataFrame.join(...).",
                line=line,
                column=column,
                snippet=line_at(text, line),
            )
        )
    return findings


def dedupe_findings(findings: Iterable[Finding]) -> list[Finding]:
    seen: set[tuple[str, int, int]] = set()
    unique: list[Finding] = []
    for finding in findings:
        key = (finding.rule_id, finding.line, finding.column)
        if key in seen:
            continue
        seen.add(key)
        unique.append(finding)
    unique.sort(key=lambda f: (f.line, f.column, f.rule_id))
    return unique


def read_input(path: str | None) -> tuple[str, str]:
    if path:
        file_path = Path(path)
        text = file_path.read_text(encoding="utf-8")
        return text, str(file_path)
    text = sys.stdin.read()
    return text, "<stdin>"


def resolve_engines(engine: str, text: str, path: str | None) -> set[str]:
    if engine == "all":
        return set(ALL_ENGINES)
    if engine == "auto":
        return detect_engines(text, path)
    return {engine}


def emit_text(findings: list[Finding], display_path: str, max_findings: int) -> None:
    if not findings:
        print(f"{display_path}: No findings.")
        return
    for finding in findings[:max_findings]:
        print(
            f"{display_path}:{finding.line}:{finding.column} "
            f"[{finding.severity}] {finding.rule_id} {finding.message}"
        )
    if len(findings) > max_findings:
        omitted = len(findings) - max_findings
        print(f"{display_path}: ... {omitted} additional findings omitted.")


def emit_json(
    findings: list[Finding], display_path: str, engines: set[str], max_findings: int
) -> None:
    payload = {
        "path": display_path,
        "engines": sorted(engines),
        "findings": [asdict(item) for item in findings[:max_findings]],
        "truncated": max(0, len(findings) - max_findings),
    }
    print(json.dumps(payload, indent=2))


def main() -> int:
    args = parse_args()
    text, display_path = read_input(args.path)
    if not text.strip():
        print(f"{display_path}: Empty input.")
        return 0

    engines = resolve_engines(args.engine, text, args.path)
    findings = dedupe_findings(
        run_regex_rules(text, engines) + run_custom_pyspark_checks(text, engines)
    )

    if args.format == "json":
        emit_json(findings, display_path, engines, args.max_findings)
    else:
        emit_text(findings, display_path, args.max_findings)

    if args.fail_on_warn and findings:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
