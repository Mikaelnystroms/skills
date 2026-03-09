#!/usr/bin/env python3
"""Static checker for common PySpark style issues.

This checker is intentionally lightweight and heuristic-based. It highlights
patterns worth reviewing against the bundled style guide.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class Rule:
    rule_id: str
    severity: str
    message: str
    pattern: str
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
        "PSG_DRIVER_ACTION",
        "warn",
        "Review driver-side actions in transform code.",
        r"\.(collect|head|take|first|show)\s*\(",
    ),
    Rule(
        "PSG_UDF",
        "warn",
        "Prefer built-in expressions over Python UDFs when possible.",
        r"(@udf\b|\b(?:F\.)?udf\s*\()",
        re.IGNORECASE,
    ),
    Rule(
        "PSG_BACKSLASH_CONTINUATION",
        "warn",
        "Prefer parentheses over backslash line continuation.",
        r"\\\s*$",
        re.MULTILINE,
    ),
    Rule(
        "PSG_OTHERWISE_FALLBACK",
        "warn",
        "Review broad otherwise(...) fallbacks to ensure unexpected values stay visible.",
        r"\.otherwise\s*\(",
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check PySpark code for common style issues.")
    parser.add_argument("--path", help="Path to file to check. If omitted, read from stdin.")
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


def run_regex_rules(text: str) -> list[Finding]:
    findings: list[Finding] = []
    for rule in RULES:
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


def run_custom_checks(text: str) -> list[Finding]:
    findings: list[Finding] = []
    for match in re.finditer(r"\.join\s*\((.*?)\)", text, flags=re.DOTALL):
        args = match.group(1)
        if "how=" in args:
            continue
        line, column = index_to_line_col(text, match.start())
        findings.append(
            Finding(
                rule_id="PSG_JOIN_WITHOUT_HOW",
                severity="warn",
                message="Specify how= explicitly in DataFrame.join(...).",
                line=line,
                column=column,
                snippet=line_at(text, line),
            )
        )

    for match in re.finditer(r"\bWindow\.(partitionBy|orderBy)\s*\(", text):
        start = match.start()
        snippet = text[start : start + 240]
        if "rowsBetween" in snippet or "rangeBetween" in snippet:
            continue
        line, column = index_to_line_col(text, start)
        findings.append(
            Finding(
                rule_id="PSG_WINDOW_FRAME_REVIEW",
                severity="warn",
                message="Review window frame semantics and make the frame explicit when it matters.",
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


def emit_json(findings: list[Finding], display_path: str, max_findings: int) -> None:
    payload = {
        "path": display_path,
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

    findings = dedupe_findings(run_regex_rules(text) + run_custom_checks(text))

    if args.format == "json":
        emit_json(findings, display_path, args.max_findings)
    else:
        emit_text(findings, display_path, args.max_findings)

    if args.fail_on_warn and findings:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
