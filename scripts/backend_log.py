#!/usr/bin/env python3
import argparse
import re
import sys
from collections import Counter
from pathlib import Path

SUCCESS = re.compile(r"^test ([^ ]+) \.\.\. ok$")


def executed_tests(text: str) -> list[str]:
    return [match.group(1) for line in text.splitlines() if (match := SUCCESS.fullmatch(line))]


def verify_log(text: str, expected: list[str]) -> list[str]:
    if not expected:
        return ["expected test list is empty"]
    duplicates = sorted(name for name, count in Counter(expected).items() if count != 1)
    actual = executed_tests(text)
    actual_counts = Counter(actual)
    missing = sorted(set(expected) - set(actual))
    unexpected = sorted(set(actual) - set(expected))
    repeated = sorted(name for name, count in actual_counts.items() if count != 1)
    errors = []
    if duplicates:
        errors.append(f"duplicate expected tests: {', '.join(duplicates)}")
    if missing:
        errors.append(f"tests did not succeed: {', '.join(missing)}")
    if unexpected:
        errors.append(f"unexpected successful tests: {', '.join(unexpected)}")
    if repeated:
        errors.append(f"tests reported success more than once: {', '.join(repeated)}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify exact backend test execution")
    parser.add_argument("log", type=Path)
    parser.add_argument("expected", nargs="+")
    args = parser.parse_args()
    errors = verify_log(args.log.read_text(encoding="utf-8"), args.expected)
    if errors:
        print("backend test verification failed:", file=sys.stderr)
        for error in errors:
            print(f"  {error}", file=sys.stderr)
        return 1
    for name in args.expected:
        print(name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
