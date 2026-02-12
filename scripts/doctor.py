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


def _run(cmd: list[str]) -> tuple[int, str, str]:
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode, result.stdout.strip(), result.stderr.strip()


def _print_output(stdout: str, stderr: str) -> None:
    if stdout:
        print(stdout)
    if stderr:
        print(stderr)


def _git_show_to_file(path: Path) -> bool:
    code, stdout, stderr = _run(["git", "show", f"HEAD:{path.as_posix()}"])
    if code != 0:
        _print_output(stdout, stderr)
        return False

    # Write exact HEAD content to disk to avoid editor/merge leftovers.
    path.write_text(stdout + ("\n" if not stdout.endswith("\n") else ""), encoding="utf-8")
    return True


def restore_files() -> int:
    print("Attempting auto-restore from git (git restore)...")
    code, stdout, stderr = _run(["git", "restore", "--source=HEAD", "--worktree", "--", *[str(path) for path in CHECK_FILES]])
    _print_output(stdout, stderr)

    print("Attempting hard content reset from HEAD (git show -> file write)...")
    ok = True
    for file_path in CHECK_FILES:
        if not _git_show_to_file(file_path):
            ok = False

    return 0 if code == 0 and ok else 1


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
    print("1) Re-sync code: git pull --ff-only")
    print("2) Close editors/terminals locking files and re-run doctor with --fix")
    print("3) Manual restore if needed:")
    print("   git restore --source=HEAD --worktree -- src/pipeline/*.py")

    if not args.fix:
        return 1

    if restore_files() != 0:
        print("Auto-restore had issues. Please run the manual restore command above.")

    print("\nRe-running syntax checks after auto-restore...")
    if syntax_ok():
        print("\nAuto-fix succeeded.")
        return 0

    print("\nAuto-fix did not resolve all syntax errors.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
