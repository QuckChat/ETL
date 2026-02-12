from pathlib import Path


def test_readme_contains_mermaid_flowchart() -> None:
    content = Path("README.md").read_text(encoding="utf-8")
    assert "```mermaid" in content
    assert "flowchart LR" in content
