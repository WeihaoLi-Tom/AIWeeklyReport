#!/usr/bin/env python3
import argparse
import datetime as dt
import html
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple


def read_jsonl(path: Path) -> Tuple[List[Dict[str, Any]], int]:
    records: List[Dict[str, Any]] = []
    skipped = 0
    if not path.exists():
        return records, skipped

    with path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
                if isinstance(item, dict):
                    records.append(item)
                else:
                    skipped += 1
            except json.JSONDecodeError:
                skipped += 1
    return records, skipped


def html_escape(value: Any) -> str:
    return html.escape(str(value), quote=True)


def is_http_url(value: str) -> bool:
    return bool(re.match(r"^https?://\S+$", value.strip()))


def parse_json_like_text(value: Any) -> Any:
    if not isinstance(value, str):
        return value

    text = value.strip()
    if not text:
        return value

    # 1) Direct JSON
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError, ValueError):
        pass

    # 2) Markdown fenced JSON anywhere in text
    fenced = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text, re.IGNORECASE)
    if fenced:
        block = fenced.group(1).strip()
        try:
            return json.loads(block)
        except (json.JSONDecodeError, TypeError, ValueError):
            pass

    # 3) reasoning model output: <think>...</think>[{...}]
    if "</think>" in text:
        tail = text.split("</think>", 1)[1].strip()
        try:
            return json.loads(tail)
        except (json.JSONDecodeError, TypeError, ValueError):
            pass

    # 4) fallback: parse from first top-level object/array token
    first_obj = text.find("{")
    first_arr = text.find("[")
    starts = [idx for idx in (first_obj, first_arr) if idx != -1]
    if starts:
        start = min(starts)
        candidate = text[start:].strip()
        try:
            return json.loads(candidate)
        except (json.JSONDecodeError, TypeError, ValueError):
            pass

    return value


def normalize_payload(value: Any) -> Any:
    value = parse_json_like_text(value)
    if isinstance(value, dict):
        return {k: normalize_payload(v) for k, v in value.items()}
    if isinstance(value, list):
        return [normalize_payload(v) for v in value]
    return value


def extract_display_payload(record: Dict[str, Any]) -> Any:
    result = record.get("result")
    payload: Any = result
    if isinstance(result, dict):
        if "output" in result:
            payload = result["output"]
        elif "answer" in result:
            payload = result["answer"]

    payload = normalize_payload(payload)
    if isinstance(payload, dict) and "answer" in payload:
        answer = normalize_payload(payload.get("answer"))
        if isinstance(answer, (list, dict)):
            return answer
    return payload


def choose_latest_success_payload(records: List[Dict[str, Any]]) -> Tuple[Any, Dict[str, Any]]:
    for record in reversed(records):
        if record.get("status") != "success":
            continue
        payload = extract_display_payload(record)
        # Skip empty payloads like {}, [], "", None
        if payload in ({}, [], "", None):
            continue
        # Skip wrapper dict without meaningful content
        if isinstance(payload, dict) and not any(payload.values()):
            continue
        return payload, record
    if records:
        return extract_display_payload(records[-1]), records[-1]
    return [], {}


def is_article_item(item: Any) -> bool:
    if not isinstance(item, dict):
        return False
    return all(key in item for key in ("category", "title", "summary"))


def to_chinese_index(i: int) -> str:
    nums = ["一", "二", "三", "四", "五", "六", "七", "八", "九", "十", "十一", "十二", "十三", "十四", "十五"]
    if 1 <= i <= len(nums):
        return nums[i - 1]
    return str(i)


def render_fallback_json(value: Any) -> str:
    pretty = json.dumps(value, ensure_ascii=False, indent=2)
    return f"<pre>{html_escape(pretty)}</pre>"


def render_article(payload: Any) -> str:
    if not isinstance(payload, list):
        return render_fallback_json(payload)

    items = [x for x in payload if is_article_item(x)]
    if not items:
        return render_fallback_json(payload)

    categories: List[str] = []
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for item in items:
        category = str(item.get("category", "其他")).strip() or "其他"
        if category not in grouped:
            grouped[category] = []
            categories.append(category)
        grouped[category].append(item)

    sections: List[str] = []
    for idx, category in enumerate(categories, start=1):
        card_items: List[str] = []
        for i, item in enumerate(grouped[category], start=1):
            title = html_escape(item.get("title", ""))
            summary = html_escape(item.get("summary", "")).replace("\n", "<br>")
            url = str(item.get("url", "")).strip()
            if is_http_url(url):
                link_html = (
                    '<div class="source">原始链接：</div>'
                    f'<a class="source-link" href="{html_escape(url)}" target="_blank" rel="noopener noreferrer">{html_escape(url)}</a>'
                )
            else:
                link_html = ""

            card_items.append(
                '<article class="news-item">'
                f'<h3>{i}. {title}</h3>'
                f'<p>{summary}</p>'
                f'{link_html}'
                '</article>'
            )

        sections.append(
            '<section class="category-section">'
            '<div class="category-title-row">'
            f'<span class="category-index">{to_chinese_index(idx)}、</span>'
            f'<span class="category-title">{html_escape(category)}</span>'
            '</div>'
            f'{"".join(card_items)}'
            '</section>'
        )

    return "".join(sections)


def render_category_overview(payload: Any) -> str:
    if not isinstance(payload, list):
        return ""
    items = [x for x in payload if is_article_item(x)]
    if not items:
        return ""

    grouped: Dict[str, int] = {}
    for item in items:
        category = str(item.get("category", "其他")).strip() or "其他"
        grouped[category] = grouped.get(category, 0) + 1

    chips = "".join(
        f'<span class="chip">{html_escape(category)} ({count})</span>'
        for category, count in grouped.items()
    )
    return f'<div class="category-overview">{chips}</div>'


def build_html(title: str, records: List[Dict[str, Any]], skipped: int) -> str:
    payload, source_record = choose_latest_success_payload(records)
    article_html = render_article(payload)
    overview_html = render_category_overview(payload)

    generated = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    started_at = source_record.get("started_at_utc", "-") if source_record else "-"

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html_escape(title)}</title>
  <style>
    :root {{
      --bg: #f2f4f7;
      --card: #ffffff;
      --text: #1f2937;
      --muted: #6b7280;
      --line: #e5e7eb;
      --primary: #0b6aa6;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif;
      line-height: 1.75;
      padding: 20px;
    }}
    .container {{ max-width: 860px; margin: 0 auto; }}
    .report-card {{
      background: var(--card);
      border-radius: 14px;
      border: 1px solid var(--line);
      padding: 24px 28px;
      box-shadow: 0 8px 24px rgba(15, 23, 42, 0.05);
    }}
    .report-header {{
      border-bottom: 1px solid var(--line);
      padding-bottom: 14px;
      margin-bottom: 18px;
    }}
    .report-header h1 {{
      margin: 0;
      text-align: center;
      font-size: clamp(30px, 4.2vw, 48px);
      font-weight: 800;
      line-height: 1.35;
      letter-spacing: 0.4px;
    }}
    .meta {{
      margin-top: 10px;
      text-align: center;
      color: var(--muted);
      font-size: 12px;
    }}
    .category-overview {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 14px;
      justify-content: center;
    }}
    .chip {{
      display: inline-flex;
      align-items: center;
      padding: 4px 10px;
      border-radius: 999px;
      background: #eef6fb;
      color: #0b6aa6;
      border: 1px solid #cde4f3;
      font-size: 12px;
      font-weight: 600;
    }}
    .category-section {{ margin-top: 20px; }}
    .category-title-row {{
      display: flex;
      align-items: center;
      gap: 6px;
      margin-bottom: 10px;
      font-size: clamp(26px, 3.2vw, 38px);
      font-weight: 800;
      color: var(--primary);
      border-left: 4px solid var(--primary);
      padding-left: 10px;
    }}
    .news-item {{
      padding: 14px 0 18px;
      border-bottom: 1px dashed #e6e6e6;
    }}
    .news-item:last-child {{ border-bottom: none; }}
    .news-item h3 {{
      margin: 0 0 8px;
      font-size: clamp(24px, 2.8vw, 36px);
      line-height: 1.45;
    }}
    .news-item p {{
      margin: 0;
      color: #374151;
      font-size: clamp(20px, 2.3vw, 30px);
    }}
    .source {{
      margin-top: 10px;
      color: #9ca3af;
      font-size: clamp(18px, 2vw, 26px);
    }}
    .source-link {{
      color: #0b6aa6;
      text-decoration: none;
      font-size: clamp(18px, 2vw, 26px);
      word-break: break-all;
    }}
    .source-link:hover {{ text-decoration: underline; }}
    pre {{
      margin: 0;
      padding: 14px;
      border-radius: 8px;
      border: 1px solid var(--line);
      background: #f8fafc;
      font-size: 12px;
      overflow: auto;
    }}
    @media (max-width: 768px) {{
      body {{ padding: 10px; line-height: 1.62; }}
      .report-card {{ padding: 14px 14px; border-radius: 10px; }}
      .report-header {{ margin-bottom: 12px; padding-bottom: 10px; }}
      .report-header h1 {{ font-size: 34px; line-height: 1.3; }}
      .meta {{ font-size: 11px; line-height: 1.5; word-break: break-word; }}
      .category-overview {{ margin-top: 10px; gap: 6px; }}
      .chip {{ font-size: 11px; padding: 3px 8px; }}
      .category-section {{ margin-top: 12px; }}
      .category-title-row {{
        font-size: 28px;
        margin-bottom: 6px;
        border-left-width: 3px;
        padding-left: 8px;
      }}
      .news-item {{ padding: 10px 0 12px; }}
      .news-item h3 {{ font-size: 24px; margin-bottom: 5px; line-height: 1.4; }}
      .news-item p {{ font-size: 19px; line-height: 1.55; }}
      .source {{ font-size: 17px; margin-top: 7px; }}
      .source-link {{ font-size: 17px; line-height: 1.45; }}
    }}
  </style>
</head>
<body>
  <div class="container">
    <article class="report-card">
      <header class="report-header">
        <h1>{html_escape(title)}</h1>
        <div class="meta">生成时间: {html_escape(generated)} | 来源记录时间: {html_escape(started_at)} | records: {len(records)} | skipped: {skipped}</div>
        {overview_html}
      </header>
      {article_html}
    </article>
  </div>
</body>
</html>
"""


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate an HTML report from scheduler JSONL output."
    )
    parser.add_argument("--input", default="workflow_runs.jsonl", help="Input JSONL path.")
    parser.add_argument("--output", default="workflow_report.html", help="Output HTML path.")
    parser.add_argument("--title", default="AI 周报", help="Report title shown in HTML.")
    parser.add_argument("--latest", type=int, default=0, help="Only include latest N runs (0 means all).")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    records, skipped = read_jsonl(input_path)
    if args.latest > 0:
        records = records[-args.latest :]

    report_html = build_html(args.title, records, skipped)
    output_path.write_text(report_html, encoding="utf-8")

    print(f"Report generated: {output_path.resolve()}")
    print(f"Input records: {len(records)}, skipped invalid lines: {skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
