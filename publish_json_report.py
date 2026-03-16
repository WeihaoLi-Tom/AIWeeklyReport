#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any, Dict, List

from dify_workflow_scheduler import load_dotenv, r2_upload_and_link


MARKDOWN_LINK_RE = re.compile(r"^\[(https?://[^\]]+)\]\((https?://[^\)]+)\)$")
DATE_RE = re.compile(r"^\d{4}[.-]\d{2}[.-]\d{2}$")


def normalize_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for item in items:
        cleaned = dict(item)
        url = str(cleaned.get("url", "")).strip()
        match = MARKDOWN_LINK_RE.match(url)
        if match:
            cleaned["url"] = match.group(2)
        normalized.append(cleaned)
    return normalized


def build_started_at(prefix: str) -> str:
    if DATE_RE.match(prefix):
        return prefix.replace(".", "-") + "T00:00:00+00:00"
    return "1970-01-01T00:00:00+00:00"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate an HTML report from weeklyAI.json and upload it to R2."
    )
    parser.add_argument(
        "--prefix",
        default="",
        help="Optional report prefix like 2026.03.09. Defaults to today's date.",
    )
    parser.add_argument(
        "--title",
        default="",
        help="Optional report title. Defaults to '<prefix> AI Weekly Report'.",
    )
    args = parser.parse_args()

    repo_dir = Path(__file__).resolve().parent
    input_path = repo_dir / "weeklyAI.json"
    output_dir = repo_dir / ".ci-output"
    output_dir.mkdir(exist_ok=True)

    load_dotenv(str(repo_dir / ".env"))
    load_dotenv(".env")

    items = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(items, list):
        raise ValueError("Input JSON must be an array of article items.")

    items = normalize_items(items)
    prefix = args.prefix.strip() or dt.datetime.now().strftime("%Y.%m.%d")
    title = args.title.strip() or f"{prefix} AI Weekly Report"

    jsonl_path = output_dir / f"{prefix}_weeklyAI.jsonl"
    html_path = output_dir / f"{prefix}_weeklyAI_report.html"

    record = {
        "status": "success",
        "started_at_utc": build_started_at(prefix),
        "result": items,
    }
    jsonl_path.write_text(
        json.dumps(record, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    subprocess.run(
        [
            "python3",
            str(repo_dir / "generate_web_report.py"),
            "--input",
            str(jsonl_path),
            "--output",
            str(html_path),
            "--title",
            title,
            "--latest",
            "1",
        ],
        check=True,
    )

    url = r2_upload_and_link(
        local_html=str(html_path),
        bucket=os.environ["R2_BUCKET_NAME"],
        key_prefix=os.environ.get("R2_KEY_PREFIX", "weekly-reports"),
        expires=int(os.environ.get("R2_PRESIGN_EXPIRES", "3600")),
    )

    if not url:
        raise RuntimeError("Failed to generate R2 link.")

    print(url)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
