"""
Extract workspaces that were processed in logs/*.log files.

Usage (from repo root):
    python SprintStories/EN-xx-goals-to-objectives/extract_runned_workspaces_from_logs.py
    python SprintStories/EN-xx-goals-to-objectives/extract_runned_workspaces_from_logs.py \
        --json-output SprintStories/EN-xx-goals-to-objectives/data/runned_workspaces_from_logs.json
"""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
LOGS_DIR = SCRIPT_DIR / "logs"
DEFAULT_OUTPUT = SCRIPT_DIR / "data" / "runned_workspaces_from_logs.json"

WORKSPACE_RE = re.compile(r"=== Workspace: (.+?) \(([0-9a-f]{24})\) ===")


def infer_script(log_path: Path) -> str:
    if "audit_model_objectives" in log_path.name:
        return "audit_model_objectives"
    if "goals_to_objectives" in log_path.name:
        return "goals_to_objectives"
    return log_path.stem


def infer_mode(text: str, script: str) -> str:
    if script == "audit_model_objectives":
        return "dry-run" if "Dry run only" in text else "apply"
    if script == "goals_to_objectives":
        if re.search(r"Updated \d+ models", text):
            return "apply"
        if "Dry run only" in text or "Re-run with --apply" in text:
            return "dry-run"
    return "unknown"


def extract_workspaces(logs_dir: Path) -> dict:
    runs: list[dict] = []
    by_workspace: dict[tuple[str, str], dict] = defaultdict(
        lambda: {
            "workspace.name": None,
            "workspace.id": None,
            "run_count": 0,
            "log_files": [],
            "scripts": set(),
            "first_seen": None,
            "last_seen": None,
            "apply_runs": 0,
            "dry_runs": 0,
        }
    )

    log_files = sorted(logs_dir.rglob("*.log"))
    for log_path in log_files:
        text = log_path.read_text(encoding="utf-8", errors="replace")
        script = infer_script(log_path)
        mode = infer_mode(text, script)
        mtime = datetime.fromtimestamp(log_path.stat().st_mtime, tz=timezone.utc).isoformat()

        seen_in_file: set[tuple[str, str]] = set()
        for match in WORKSPACE_RE.finditer(text):
            name, workspace_id = match.group(1), match.group(2)
            key = (name, workspace_id)
            if key in seen_in_file:
                continue
            seen_in_file.add(key)

            rel_log = str(log_path.relative_to(SCRIPT_DIR))
            runs.append(
                {
                    "workspace.name": name,
                    "workspace.id": workspace_id,
                    "log_file": rel_log,
                    "script": script,
                    "log_modified_at": mtime,
                    "mode": mode,
                }
            )

            ws = by_workspace[key]
            ws["workspace.name"] = name
            ws["workspace.id"] = workspace_id
            ws["run_count"] += 1
            ws["log_files"].append(rel_log)
            ws["scripts"].add(script)
            ws["first_seen"] = min(filter(None, [ws["first_seen"], mtime]), default=mtime)
            ws["last_seen"] = max(filter(None, [ws["last_seen"], mtime]), default=mtime)
            if mode == "apply":
                ws["apply_runs"] += 1
            elif mode == "dry-run":
                ws["dry_runs"] += 1

    workspaces = [
        {
            "workspace.name": ws["workspace.name"],
            "workspace.id": ws["workspace.id"],
            "run_count": ws["run_count"],
            "apply_runs": ws["apply_runs"],
            "dry_runs": ws["dry_runs"],
            "scripts": sorted(ws["scripts"]),
            "first_seen": ws["first_seen"],
            "last_seen": ws["last_seen"],
            "log_files": sorted(set(ws["log_files"])),
        }
        for ws in sorted(by_workspace.values(), key=lambda item: item["workspace.name"].lower())
    ]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "logs_scanned": len(log_files),
        "workspace_count": len(workspaces),
        "run_entries": len(runs),
        "workspaces": workspaces,
        "runs": runs,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract run workspaces from log files.")
    parser.add_argument(
        "--logs-dir",
        type=Path,
        default=LOGS_DIR,
        help=f"Directory containing log files (default: {LOGS_DIR.name}/)",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"JSON output path (default: {DEFAULT_OUTPUT.name})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report = extract_workspaces(args.logs_dir)
    args.json_output.parent.mkdir(parents=True, exist_ok=True)
    args.json_output.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"Wrote {args.json_output}")
    print(f"Workspaces ({report['workspace_count']}):")
    for workspace in report["workspaces"]:
        print(
            f"  {workspace['workspace.name']} ({workspace['workspace.id']}) "
            f"- {workspace['run_count']} runs"
        )


if __name__ == "__main__":
    main()
