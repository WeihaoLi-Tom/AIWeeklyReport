#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import re
import signal
import subprocess
import sys
import time
import webbrowser
from typing import Any, Dict, Optional

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False
    from urllib import error, request

try:
    import boto3
    from botocore.config import Config as BotoConfig
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

try:
    import schedule as _schedule_lib
    HAS_SCHEDULE = True
except ImportError:
    HAS_SCHEDULE = False


# ---------------------------------------------------------------------------
# env helpers
# ---------------------------------------------------------------------------

def load_dotenv(path: str = ".env") -> bool:
    if not os.path.exists(path):
        return False
    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'").strip('"')
            if key and key not in os.environ:
                os.environ[key] = value
    return True


def require_env(name: str, hint: str = "") -> str:
    value = os.getenv(name, "").strip()
    if not value:
        message = f"Missing required env: {name}"
        if hint:
            message = f"{message}. {hint}"
        raise ValueError(message)
    return value


def parse_json_env(name: str, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default or {}
    try:
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError(f"{name} must be a JSON object")
        return data
    except json.JSONDecodeError as ex:
        raise ValueError(f"Invalid JSON in {name}: {ex}") from ex


def parse_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    return raw in ("true", "1", "yes", "y", "on")


def parse_schedule_time(value: str) -> str:
    text = value.strip()
    if not re.match(r"^([01]\d|2[0-3]):([0-5]\d)$", text):
        raise ValueError("SCHEDULE_TIME must be HH:MM, e.g. 09:00")
    return text


# ---------------------------------------------------------------------------
# Dify helpers
# ---------------------------------------------------------------------------

def build_query(query_template: str, run_index: int) -> str:
    now = dt.datetime.now(dt.timezone.utc)
    return query_template.format(
        triggered_at_utc=now.isoformat(),
        run_index=run_index,
        timestamp=now.strftime("%Y-%m-%d %H:%M:%S UTC"),
    )


def build_inputs(base_inputs: Dict[str, Any], run_index: int) -> Dict[str, Any]:
    now = dt.datetime.now(dt.timezone.utc)
    dynamic = {
        "triggered_at_utc": now.isoformat(),
        "run_index": run_index,
    }
    merged = dict(base_inputs)
    merged.update(dynamic)
    return merged


def parse_json_from_text(value: Any) -> Any:
    if not isinstance(value, str):
        return value

    text = value.strip()
    if not text:
        return value

    if text.startswith("```"):
        lines = text.split("\n")
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError, ValueError):
        return value


def extract_structured_output(result: Dict[str, Any]) -> Any:
    workflow_outputs = result.get("workflow_outputs")
    if workflow_outputs is not None:
        return workflow_outputs

    data = result.get("data")
    if isinstance(data, dict) and "outputs" in data:
        return data.get("outputs")

    if "answer" in result:
        return parse_json_from_text(result.get("answer"))

    return result


def call_dify_app(
    base_url: str,
    api_key: str,
    app_type: str,
    query: str,
    inputs: Dict[str, Any],
    user: str,
    conversation_id: Optional[str] = None,
    response_mode: str = "blocking",
) -> Dict[str, Any]:
    if app_type.lower() == "workflow":
        endpoint = f"{base_url.rstrip('/')}/workflows/run"
        payload = {
            "inputs": inputs,
            "response_mode": response_mode,
            "user": user,
        }
    elif app_type.lower() == "chat":
        endpoint = f"{base_url.rstrip('/')}/chat-messages"
        payload = {
            "query": query,
            "response_mode": response_mode,
            "user": user,
        }
        if inputs:
            payload["inputs"] = inputs
        if conversation_id:
            payload["conversation_id"] = conversation_id
    else:
        raise ValueError(f"Unknown app_type: {app_type}. Must be 'workflow' or 'chat'.")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, */*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    }

    if HAS_REQUESTS:
        try:
            if response_mode == "streaming":
                resp = requests.post(
                    endpoint,
                    json=payload,
                    headers=headers,
                    timeout=180,
                    stream=True,
                )
                resp.raise_for_status()

                result: Dict[str, Any] = {"events": []}
                full_answer = ""
                last_metadata = None
                workflow_outputs = None
                all_node_outputs = []

                print("\n" + "=" * 60)
                print("Stream output:")
                print("=" * 60)

                for line in resp.iter_lines(decode_unicode=True):
                    if not line or not line.startswith("data: "):
                        continue

                    data_str = line[6:].strip()
                    if not data_str:
                        continue

                    try:
                        event = json.loads(data_str)
                        event_type = event.get("event")

                        if event_type == "message":
                            answer_chunk = event.get("answer", "")
                            if answer_chunk:
                                print(answer_chunk, end="", flush=True)
                                full_answer += str(answer_chunk)
                        elif event_type == "message_end":
                            last_metadata = event.get("metadata")
                            result["conversation_id"] = event.get("conversation_id")
                            result["message_id"] = event.get("message_id")
                            print("\n" + "=" * 60)
                        elif event_type == "workflow_started":
                            print(f"[Workflow started: {event.get('workflow_run_id')}]")
                        elif event_type == "workflow_finished":
                            workflow_data = event.get("data", {})
                            workflow_outputs = workflow_data.get("outputs")
                            if workflow_outputs:
                                print("\n[Workflow outputs captured]")
                        elif event_type == "node_started":
                            node_title = event.get("data", {}).get("title", "Unknown")
                            print(f"[Node: {node_title}]", flush=True)
                        elif event_type == "node_finished":
                            node_data = event.get("data", {})
                            node_title = node_data.get("title", "")
                            node_outputs = node_data.get("outputs")

                            if node_outputs:
                                for key, value in node_outputs.items():
                                    if value and value != "":
                                        content_size = 0
                                        if isinstance(value, str):
                                            content_size = len(value)
                                        elif isinstance(value, (dict, list)):
                                            content_size = len(json.dumps(value))

                                        if content_size > 50:
                                            all_node_outputs.append({
                                                "node": node_title,
                                                "key": key,
                                                "value": value,
                                                "size": content_size,
                                            })
                                            print(f"\n[Node output: {node_title}.{key} ({content_size} chars)]")

                        result["events"].append(event)
                    except json.JSONDecodeError:
                        continue

                if full_answer and len(full_answer) > 500:
                    print(f"\n[Using message answer ({len(full_answer)} chars)]")
                elif all_node_outputs:
                    all_node_outputs.sort(key=lambda x: x["size"], reverse=True)
                    largest_output = all_node_outputs[0]
                    if len(largest_output["value"]) > len(full_answer):
                        full_answer = largest_output["value"]
                        print(f"\n[Selected output from: {largest_output['node']}.{largest_output['key']} ({largest_output['size']} chars)]")
                elif workflow_outputs and not full_answer:
                    if isinstance(workflow_outputs, dict):
                        for key in ["text", "output", "result", "answer"]:
                            if key in workflow_outputs:
                                full_answer = workflow_outputs[key]
                                break
                        if not full_answer:
                            full_answer = workflow_outputs
                    else:
                        full_answer = workflow_outputs

                result["answer"] = full_answer
                if workflow_outputs is not None:
                    result["workflow_outputs"] = workflow_outputs
                if last_metadata:
                    result["metadata"] = last_metadata

                return result
            else:
                resp = requests.post(
                    endpoint,
                    json=payload,
                    headers=headers,
                    timeout=180,
                )
                resp.raise_for_status()
                return resp.json()
        except requests.exceptions.HTTPError as ex:
            detail = ex.response.text
            try:
                error_payload = ex.response.json()
                if isinstance(error_payload, dict):
                    code = error_payload.get("code")
                    message = error_payload.get("message") or error_payload.get("error")
                    if code or message:
                        detail = f"code={code}, message={message}, raw={ex.response.text}"
            except (json.JSONDecodeError, ValueError):
                pass
            raise RuntimeError(f"HTTP {ex.response.status_code}: {detail}") from ex
        except requests.exceptions.RequestException as ex:
            raise RuntimeError(f"Request error: {ex}") from ex
    else:
        body = json.dumps(payload).encode("utf-8")
        req = request.Request(endpoint, data=body, method="POST")
        for key, value in headers.items():
            req.add_header(key, value)

        try:
            with request.urlopen(req, timeout=180) as resp:
                data = resp.read().decode("utf-8")
                return json.loads(data)
        except error.HTTPError as ex:
            raw = ex.read().decode("utf-8", errors="replace")
            detail = raw
            try:
                error_payload = json.loads(raw)
                if isinstance(error_payload, dict):
                    code = error_payload.get("code")
                    message = error_payload.get("message") or error_payload.get("error")
                    if code or message:
                        detail = f"code={code}, message={message}, raw={raw}"
            except json.JSONDecodeError:
                pass
            raise RuntimeError(f"HTTP {ex.code}: {detail}") from ex
        except error.URLError as ex:
            raise RuntimeError(f"Network error: {ex}") from ex


# ---------------------------------------------------------------------------
# storage helpers
# ---------------------------------------------------------------------------

def append_json_line(path: str, item: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")


def generate_html_report(
    script_dir: str,
    input_jsonl: str,
    output_html: str,
    title: str,
    latest: int,
) -> bool:
    report_script = os.path.join(script_dir, "generate_web_report.py")
    if not os.path.exists(report_script):
        print(f"[Report] skipped: script not found at {report_script}", file=sys.stderr)
        return False

    cmd = [
        sys.executable,
        report_script,
        "--input", input_jsonl,
        "--output", output_html,
        "--title", title,
    ]
    if latest > 0:
        cmd.extend(["--latest", str(latest)])

    try:
        completed = subprocess.run(cmd, check=True, capture_output=True, text=True)
        if completed.stdout.strip():
            print(completed.stdout.strip())
        return True
    except subprocess.CalledProcessError as ex:
        print(
            f"[Report] generation failed (code={ex.returncode}): "
            f"{(ex.stderr or ex.stdout or '').strip()}",
            file=sys.stderr,
        )
        return False


def maybe_open_report_in_browser(report_path: str, enabled: bool) -> None:
    if not enabled:
        return
    try:
        webbrowser.open(f"file://{os.path.abspath(report_path)}", new=0, autoraise=True)
    except Exception as ex:
        print(f"[Report] auto-open failed: {ex}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Cloudflare R2
# ---------------------------------------------------------------------------

def build_r2_client(verify_ssl: bool = True):
    if not HAS_BOTO3:
        raise RuntimeError("boto3 is required for R2 upload. Install: pip install boto3")

    account_id = require_env("R2_ACCOUNT_ID", hint="Cloudflare account ID")
    access_key = require_env("R2_ACCESS_KEY_ID", hint="R2 API access key")
    secret_key = require_env("R2_SECRET_ACCESS_KEY", hint="R2 API secret key")
    endpoint = f"https://{account_id}.r2.cloudflarestorage.com"

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=BotoConfig(signature_version="s3v4"),
        region_name="auto",
        verify=verify_ssl,
    )


def upload_to_r2(local_path: str, bucket: str, key: str, verify_ssl: bool = True) -> None:
    client = build_r2_client(verify_ssl=verify_ssl)
    client.upload_file(
        local_path,
        bucket,
        key,
        ExtraArgs={"ContentType": "text/html; charset=utf-8"},
    )
    print(f"[R2] Uploaded {local_path} -> s3://{bucket}/{key}")


def generate_presigned_url(bucket: str, key: str, expires: int = 3600, verify_ssl: bool = True) -> str:
    client = build_r2_client(verify_ssl=verify_ssl)
    url = client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires,
    )
    return url


def r2_upload_and_presign(
    local_html: str,
    bucket: str,
    key_prefix: str,
    expires: int,
) -> Optional[str]:
    now_str = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
    object_key = f"{key_prefix}/report_{now_str}.html" if key_prefix else f"report_{now_str}.html"
    allow_insecure_fallback = parse_bool_env("R2_ALLOW_INSECURE_FALLBACK", default=True)

    attempts = [True]
    if allow_insecure_fallback:
        attempts.append(False)

    last_error: Optional[Exception] = None
    for verify_ssl in attempts:
        try:
            if not verify_ssl:
                print("[R2] retrying with insecure SSL fallback...", file=sys.stderr)
            upload_to_r2(local_html, bucket, object_key, verify_ssl=verify_ssl)
            presigned = generate_presigned_url(
                bucket=bucket,
                key=object_key,
                expires=expires,
                verify_ssl=verify_ssl,
            )
            print(f"[R2] Presigned URL (valid {expires}s):\n{presigned}")
            print(f"[R2_LINK]{presigned}")
            return presigned
        except Exception as ex:
            last_error = ex
            message = str(ex)
            if verify_ssl and "SSL validation failed" in message and allow_insecure_fallback:
                print(f"[R2] SSL verification failed, fallback enabled: {message}", file=sys.stderr)
                continue
            break

    if last_error:
        print(f"[R2] Upload/presign failed: {last_error}", file=sys.stderr)
    return None


# ---------------------------------------------------------------------------
# single run pipeline
# ---------------------------------------------------------------------------

class AppConfig:
    def __init__(self, script_dir: str):
        self.script_dir = script_dir
        self.base_url = os.getenv("DIFY_BASE_URL", "https://api.dify.ai/v1")
        self.api_key = require_env(
            "DIFY_API_KEY",
            hint="Set DIFY_API_KEY in .env",
        )
        self.app_type = os.getenv("DIFY_APP_TYPE", "chat").lower()
        self.user = os.getenv("DIFY_USER", "scheduler-bot")
        self.response_mode = os.getenv("DIFY_RESPONSE_MODE", "blocking")
        self.base_inputs = parse_json_env("DIFY_INPUTS", default={})
        self.query_template = os.getenv(
            "DIFY_QUERY",
            "Generate AI weekly report for {timestamp}. Run index: {run_index}",
        )
        self.persist_conversation = parse_bool_env("DIFY_PERSIST_CONVERSATION", default=False)
        self.save_answer_only = parse_bool_env("DIFY_SAVE_ANSWER_ONLY", default=True)

        self.output_file = os.path.abspath(os.getenv("OUTPUT_FILE", "workflow_runs.jsonl"))
        self.report_auto_generate = parse_bool_env("REPORT_AUTO_GENERATE", default=True)
        self.report_output_file = os.path.abspath(
            os.getenv("REPORT_OUTPUT_FILE", "workflow_report.html")
        )
        self.report_title = os.getenv("REPORT_TITLE", "AI Weekly Report").strip() or "AI Weekly Report"
        self.report_auto_open = parse_bool_env("REPORT_AUTO_OPEN", default=True)
        try:
            self.report_latest = int(os.getenv("REPORT_LATEST", "0").strip())
        except ValueError:
            self.report_latest = 0

        self.r2_enabled = parse_bool_env("R2_ENABLED", default=False)
        self.r2_bucket = os.getenv("R2_BUCKET_NAME", "").strip()
        self.r2_key_prefix = os.getenv("R2_KEY_PREFIX", "weekly-reports").strip()
        try:
            self.r2_presign_expires = int(os.getenv("R2_PRESIGN_EXPIRES", "3600").strip())
        except ValueError:
            self.r2_presign_expires = 3600


def run_once(cfg: AppConfig, run_index: int, conversation_id: Optional[str] = None) -> Dict[str, Any]:
    started_at = dt.datetime.now(dt.timezone.utc).isoformat()
    inputs = build_inputs(cfg.base_inputs, run_index)
    query = build_query(cfg.query_template, run_index)

    run_record: Dict[str, Any] = {
        "run_index": run_index,
        "started_at_utc": started_at,
        "query": query,
        "inputs": inputs,
    }
    if conversation_id:
        run_record["conversation_id"] = conversation_id

    # Step 1: call Dify
    try:
        result = call_dify_app(
            base_url=cfg.base_url,
            api_key=cfg.api_key,
            app_type=cfg.app_type,
            query=query,
            inputs=inputs,
            user=cfg.user,
            conversation_id=conversation_id if cfg.persist_conversation else None,
            response_mode=cfg.response_mode,
        )

        answer_raw = result.get("answer", "")
        print(f"\n[Debug] Answer length: {len(answer_raw) if isinstance(answer_raw, str) else 0} chars")
        structured_output = extract_structured_output(result)

        clean_result = {
            "output": structured_output,
            "conversation_id": result.get("conversation_id"),
            "message_id": result.get("message_id"),
            "metadata": result.get("metadata"),
        }

        run_record["status"] = "success"
        if cfg.save_answer_only:
            run_record["result"] = structured_output
        else:
            run_record["result"] = clean_result
            run_record["dify_raw_result"] = result

        if cfg.app_type == "chat" and cfg.persist_conversation:
            new_conv_id = result.get("conversation_id")
            if new_conv_id:
                run_record["_next_conversation_id"] = new_conv_id

        print(f"\n[run {run_index}] success")
        print(json.dumps(clean_result, ensure_ascii=False, indent=2))
    except Exception as ex:
        run_record["status"] = "failed"
        run_record["error"] = str(ex)
        print(f"[run {run_index}] failed: {ex}", file=sys.stderr)

    # Step 2: generate HTML report
    if run_record.get("status") == "success" and cfg.report_auto_generate:
        html_ok = generate_html_report(
            script_dir=cfg.script_dir,
            input_jsonl=cfg.output_file,
            output_html=cfg.report_output_file,
            title=cfg.report_title,
            latest=cfg.report_latest,
        )

        # Step 4: upload to R2 + generate presigned URL
        if html_ok and cfg.r2_enabled:
            presigned = r2_upload_and_presign(
                local_html=cfg.report_output_file,
                bucket=cfg.r2_bucket,
                key_prefix=cfg.r2_key_prefix,
                expires=cfg.r2_presign_expires,
            )
            if presigned:
                run_record["presigned_url"] = presigned

        # Step 5: open in browser (local dev convenience)
        maybe_open_report_in_browser(cfg.report_output_file, enabled=cfg.report_auto_open)

    # Step 6: save JSONL (after optional presigned URL enrichment)
    append_json_line(cfg.output_file, run_record)

    return run_record


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> int:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env_paths = [
        os.path.join(script_dir, ".env"),
        os.path.abspath(".env"),
    ]
    loaded_paths = []
    for env_path in env_paths:
        if load_dotenv(env_path):
            loaded_paths.append(env_path)

    parser = argparse.ArgumentParser(
        description="Dify AI Weekly Report: trigger, generate HTML, upload to R2."
    )
    parser.add_argument(
        "--mode",
        choices=["once", "interval", "weekly", "monthly", "date"],
        default=os.getenv("SCHEDULE_MODE", "once").strip().lower(),
        help="once|interval|weekly|monthly|date scheduling modes.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=int(os.getenv("INTERVAL_SECONDS", "300")),
        help="Seconds between runs (interval mode only).",
    )
    args = parser.parse_args()

    cfg = AppConfig(script_dir)

    if loaded_paths:
        print(f"Loaded env from: {', '.join(loaded_paths)}")

    print(f"Mode: {args.mode}, App: {cfg.app_type}, R2: {'on' if cfg.r2_enabled else 'off'}")
    print(
        f"Report: auto_generate={'on' if cfg.report_auto_generate else 'off'}, "
        f"auto_open={'on' if cfg.report_auto_open else 'off'}, "
        f"output={cfg.report_output_file}"
    )

    stop = False

    def handle_signal(signum: int, frame: Any) -> None:
        del signum, frame
        nonlocal stop
        stop = True
        print("\nStopping scheduler...")

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # ---- once mode ----
    if args.mode == "once":
        run_once(cfg, run_index=1)
        return 0

    # ---- interval mode ----
    if args.mode == "interval":
        run_index = 1
        conversation_id: Optional[str] = None
        print(f"Interval mode: every {args.interval}s. Ctrl+C to stop.")
        while not stop:
            record = run_once(cfg, run_index, conversation_id)
            conversation_id = record.get("_next_conversation_id", conversation_id)
            run_index += 1
            for _ in range(args.interval):
                if stop:
                    break
                time.sleep(1)
        return 0

    # ---- weekly mode ----
    if args.mode == "weekly":
        if not HAS_SCHEDULE:
            print("ERROR: 'schedule' library required for weekly mode. Install: pip install schedule", file=sys.stderr)
            return 1

        schedule_time = parse_schedule_time(os.getenv("SCHEDULE_TIME", "09:00"))
        schedule_weekday = os.getenv("SCHEDULE_WEEKDAY", "monday").strip().lower()
        weekday_attrs = {
            "monday": "monday",
            "tuesday": "tuesday",
            "wednesday": "wednesday",
            "thursday": "thursday",
            "friday": "friday",
            "saturday": "saturday",
            "sunday": "sunday",
        }
        if schedule_weekday not in weekday_attrs:
            print("ERROR: SCHEDULE_WEEKDAY must be one of monday..sunday", file=sys.stderr)
            return 1

        run_state = {"index": 1, "conv_id": None}

        def weekly_job() -> None:
            print(f"\n{'=' * 60}")
            print(f"Weekly job triggered at {dt.datetime.now().isoformat()}")
            print(f"{'=' * 60}")
            record = run_once(cfg, run_state["index"], run_state["conv_id"])
            run_state["conv_id"] = record.get("_next_conversation_id", run_state["conv_id"])
            run_state["index"] += 1

        weekly_job_builder = getattr(_schedule_lib.every(), weekday_attrs[schedule_weekday])
        weekly_job_builder.at(schedule_time, "Asia/Shanghai").do(weekly_job)
        print(
            f"Weekly mode: every {schedule_weekday.title()} at {schedule_time} "
            "(Asia/Shanghai). Ctrl+C to stop."
        )
        print(f"Waiting for next {schedule_weekday.title()} {schedule_time}...")

        while not stop:
            _schedule_lib.run_pending()
            time.sleep(1)

        return 0

    # ---- monthly mode ----
    if args.mode == "monthly":
        schedule_time = parse_schedule_time(os.getenv("SCHEDULE_TIME", "09:00"))
        month_day_raw = os.getenv("SCHEDULE_MONTHDAY", "1").strip()
        try:
            month_day = int(month_day_raw)
        except ValueError:
            print("ERROR: SCHEDULE_MONTHDAY must be an integer in 1..31", file=sys.stderr)
            return 1
        if month_day < 1 or month_day > 31:
            print("ERROR: SCHEDULE_MONTHDAY must be in 1..31", file=sys.stderr)
            return 1

        run_state = {"index": 1, "conv_id": None}
        last_run_date = None
        print(
            f"Monthly mode: day {month_day} at {schedule_time} (Asia/Shanghai). "
            "Ctrl+C to stop."
        )

        while not stop:
            now = dt.datetime.now(dt.timezone(dt.timedelta(hours=8)))
            now_hhmm = now.strftime("%H:%M")
            today = now.date().isoformat()
            if now.day == month_day and now_hhmm == schedule_time and today != last_run_date:
                print(f"\n{'=' * 60}")
                print(f"Monthly job triggered at {now.isoformat()}")
                print(f"{'=' * 60}")
                record = run_once(cfg, run_state["index"], run_state["conv_id"])
                run_state["conv_id"] = record.get("_next_conversation_id", run_state["conv_id"])
                run_state["index"] += 1
                last_run_date = today
            time.sleep(1)
        return 0

    # ---- date mode (run once at specific date+time in Beijing) ----
    if args.mode == "date":
        schedule_time = parse_schedule_time(os.getenv("SCHEDULE_TIME", "09:00"))
        schedule_date = os.getenv("SCHEDULE_DATE", "").strip()
        if not schedule_date:
            print("ERROR: SCHEDULE_DATE is required in date mode (YYYY-MM-DD).", file=sys.stderr)
            return 1
        try:
            target_date = dt.datetime.strptime(schedule_date, "%Y-%m-%d").date()
        except ValueError:
            print("ERROR: SCHEDULE_DATE must be YYYY-MM-DD.", file=sys.stderr)
            return 1

        hour, minute = schedule_time.split(":")
        target_dt = dt.datetime(
            year=target_date.year,
            month=target_date.month,
            day=target_date.day,
            hour=int(hour),
            minute=int(minute),
            second=0,
            tzinfo=dt.timezone(dt.timedelta(hours=8)),
        )
        now = dt.datetime.now(dt.timezone(dt.timedelta(hours=8)))
        if now >= target_dt:
            print(
                f"ERROR: target datetime already passed ({target_dt.isoformat()}).",
                file=sys.stderr,
            )
            return 1

        print(f"Date mode: waiting until {target_dt.isoformat()} (Asia/Shanghai). Ctrl+C to stop.")
        while not stop:
            now = dt.datetime.now(dt.timezone(dt.timedelta(hours=8)))
            if now >= target_dt:
                run_once(cfg, run_index=1)
                return 0
            time.sleep(1)
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
