"""Environment/source sanity checks for local ETL execution."""

from __future__ import annotations

import argparse
import py_compile
import subprocess
from pathlib import Path

CHECK_FILES = [
    Path("src/pipeline/config.py"),
    Path("src/pipeline/ingestion.py"),
    Path("src/pipeline/job.py"),
    Path("src/pipeline/load.py"),
    Path("src/pipeline/quality.py"),
    Path("src/pipeline/transformation.py"),
]


def _run(cmd: list[str]) -> int:
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout.strip())
    if result.stderr:
        print(result.stderr.strip())
    return result.returncode


def restore_files() -> int:
    cmd = ["git", "checkout", "--", *[str(path) for path in CHECK_FILES]]
    print("Attempting auto-restore from git...")
    return _run(cmd)


def syntax_ok() -> bool:
    failed = False
    for file_path in CHECK_FILES:
        try:
            py_compile.compile(str(file_path), doraise=True)
            print(f"OK: syntax valid -> {file_path}")
        except py_compile.PyCompileError as exc:
            failed = True
            print(f"ERROR: syntax invalid -> {file_path}")
            print(exc.msg)
    return not failed


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate and optionally repair ETL source files")
    parser.add_argument(
        "--fix",
        action="store_true",
        help="If syntax check fails, restore known files from git and re-run checks.",
    )
    args = parser.parse_args()

    if syntax_ok():
        print("\nAll syntax checks passed.")
        return 0

    print("\nSuggested fixes:")
    print("1) Re-sync code: git pull")
    print("2) Restore corrupted files:")
    print("   git checkout -- src/pipeline/*.py")
    print("3) Re-run this doctor script.")

    if not args.fix:
        return 1

    if restore_files() != 0:
        print("Auto-restore failed. Please run the restore command manually.")
        return 1

    print("\nRe-running syntax checks after auto-restore...")
    if syntax_ok():
        print("\nAuto-fix succeeded.")
        return 0

    print("\nAuto-fix did not resolve all syntax errors.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
