#!/usr/bin/env python3
import datetime as dt
import os
import queue
import re
import subprocess
import sys
import threading
import time
import tkinter as tk
import webbrowser
from tkinter import messagebox, ttk
from typing import Dict, List, Optional


TIME_PATTERN = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
URL_PATTERN = re.compile(r"https?://\S+")
R2_LINK_PATTERN = re.compile(r"\[R2_LINK\](https?://\S+)")
WEEKDAYS = [
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
]


def load_env_file(path: str) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not os.path.exists(path):
        return values
    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    return values


def upsert_env_file(path: str, updates: Dict[str, str]) -> None:
    lines: List[str] = []
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()

    found_keys = set()
    new_lines: List[str] = []
    for raw_line in lines:
        stripped = raw_line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key = stripped.split("=", 1)[0].strip()
            if key in updates:
                new_lines.append(f"{key}={updates[key]}\n")
                found_keys.add(key)
                continue
        new_lines.append(raw_line)

    missing = [k for k in updates.keys() if k not in found_keys]
    if missing:
        if new_lines and new_lines[-1].strip():
            new_lines.append("\n")
        new_lines.append("# Scheduler UI updates\n")
        for key in missing:
            new_lines.append(f"{key}={updates[key]}\n")

    with open(path, "w", encoding="utf-8") as f:
        f.writelines(new_lines)


class SettingsApp:
    def __init__(self, root: tk.Tk, project_dir: str) -> None:
        self.root = root
        self.project_dir = project_dir
        self.env_path = os.path.join(project_dir, ".env")
        self.scheduler_path = os.path.join(project_dir, "dify_workflow_scheduler.py")

        self.root.title("AI Report Scheduler")
        self.root.geometry("980x780")
        self.root.minsize(860, 680)
        self.root.resizable(True, True)

        self.date_options = self._build_date_options(90)
        self.event_queue: queue.Queue = queue.Queue()
        self.scheduler_thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()
        self.next_trigger_dt: Optional[dt.datetime] = None
        self.latest_link: str = ""
        self.current_process: Optional[subprocess.Popen] = None

        self._build_style()
        self._build_ui()
        self._load_current_values()
        self._poll_events()
        self._refresh_countdown()

    def _build_style(self) -> None:
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("TFrame", background="#f6f8fb")
        style.configure("Card.TFrame", background="#ffffff")
        style.configure("TLabel", background="#ffffff", foreground="#0f172a", font=("Arial", 11))
        style.configure("Header.TLabel", background="#ffffff", font=("Arial", 15, "bold"))
        style.configure("Hint.TLabel", background="#ffffff", foreground="#64748b", font=("Arial", 10))
        style.configure("Link.TLabel", background="#ffffff", foreground="#2563eb", font=("Arial", 10, "underline"))
        style.configure("TButton", font=("Arial", 11), padding=8)
        style.configure("Primary.TButton", font=("Arial", 11, "bold"))

    def _build_ui(self) -> None:
        container = ttk.Frame(self.root, style="TFrame", padding=18)
        container.pack(fill="both", expand=True)

        card = ttk.Frame(container, style="Card.TFrame", padding=18)
        card.pack(fill="both", expand=True)

        ttk.Label(card, text="Trigger Settings", style="Header.TLabel").pack(anchor="w")
        ttk.Label(
            card,
            text="Set trigger cycle, date/time, link expiry, and monitor live stream output.",
            style="Hint.TLabel",
        ).pack(anchor="w", pady=(4, 16))

        form = ttk.Frame(card, style="Card.TFrame")
        form.pack(fill="x")

        ttk.Label(form, text="Trigger cycle:").grid(row=0, column=0, sticky="w", pady=7)
        self.mode_var = tk.StringVar()
        self.mode_combo = ttk.Combobox(
            form,
            textvariable=self.mode_var,
            values=["single", "weekly"],
            state="readonly",
            width=20,
        )
        self.mode_combo.grid(row=0, column=1, sticky="w", padx=(10, 0))
        self.mode_combo.bind("<<ComboboxSelected>>", lambda _e: self._toggle_mode_fields())

        ttk.Label(form, text="Weekday (weekly):").grid(row=1, column=0, sticky="w", pady=7)
        self.weekday_var = tk.StringVar()
        self.weekday_combo = ttk.Combobox(
            form,
            textvariable=self.weekday_var,
            values=WEEKDAYS,
            state="readonly",
            width=20,
        )
        self.weekday_combo.grid(row=1, column=1, sticky="w", padx=(10, 0))

        ttk.Label(form, text="Specific date (single):").grid(row=2, column=0, sticky="w", pady=7)
        self.date_var = tk.StringVar()
        self.date_combo = ttk.Combobox(
            form,
            textvariable=self.date_var,
            values=self.date_options,
            state="readonly",
            width=20,
        )
        self.date_combo.grid(row=2, column=1, sticky="w", padx=(10, 0))

        ttk.Label(form, text="Trigger time (HH:MM):").grid(row=3, column=0, sticky="w", pady=7)
        self.time_var = tk.StringVar()
        self.time_entry = ttk.Entry(form, textvariable=self.time_var, width=22)
        self.time_entry.grid(row=3, column=1, sticky="w", padx=(10, 0))

        ttk.Label(form, text="Link expiry (minutes):").grid(row=4, column=0, sticky="w", pady=7)
        self.expiry_var = tk.StringVar()
        self.expiry_entry = ttk.Entry(form, textvariable=self.expiry_var, width=22)
        self.expiry_entry.grid(row=4, column=1, sticky="w", padx=(10, 0))

        self.status_var = tk.StringVar(value="Ready")
        ttk.Label(card, textvariable=self.status_var, style="Hint.TLabel").pack(anchor="w", pady=(14, 4))
        self.countdown_var = tk.StringVar(value="Next trigger: -")
        ttk.Label(card, textvariable=self.countdown_var, style="Hint.TLabel").pack(anchor="w", pady=(0, 8))

        button_row = ttk.Frame(card, style="Card.TFrame")
        button_row.pack(fill="x", pady=(8, 0))

        ttk.Button(button_row, text="Save Settings", command=self.save_settings).pack(side="left")
        ttk.Button(button_row, text="Start Trigger", style="Primary.TButton", command=self.start_trigger).pack(side="left", padx=10)
        ttk.Button(button_row, text="Stop Trigger", command=self.stop_trigger).pack(side="left", padx=(0, 10))
        ttk.Button(button_row, text="Run Once Now", command=self.run_once_now).pack(side="left")

        link_row = ttk.Frame(card, style="Card.TFrame")
        link_row.pack(fill="x", pady=(12, 0))
        ttk.Label(link_row, text="Last temporary link:", style="Hint.TLabel").pack(anchor="w")
        self.link_var = tk.StringVar(value="-")
        self.link_label = ttk.Label(link_row, textvariable=self.link_var, style="Link.TLabel", cursor="hand2")
        self.link_label.pack(anchor="w", pady=(2, 2))
        self.link_label.bind("<Button-1>", self.open_latest_link)

        ttk.Label(card, text="Stream output:", style="Hint.TLabel").pack(anchor="w", pady=(8, 4))
        self.log_text = tk.Text(
            card,
            height=18,
            wrap="word",
            bg="#0b1220",
            fg="#e5e7eb",
            insertbackground="#e5e7eb",
            font=("Menlo", 10),
        )
        self.log_text.pack(fill="both", expand=True)

    def _load_current_values(self) -> None:
        values = load_env_file(self.env_path)
        mode_raw = values.get("SCHEDULE_MODE", "weekly").strip().lower()
        self.mode_var.set("single" if mode_raw == "date" else "weekly")

        self.weekday_var.set(values.get("SCHEDULE_WEEKDAY", "monday"))

        date_val = values.get("SCHEDULE_DATE", self.date_options[0] if self.date_options else "")
        if DATE_PATTERN.match(date_val) and date_val not in self.date_options:
            self.date_options = [date_val] + self.date_options
            self.date_combo.configure(values=self.date_options)
        self.date_var.set(date_val)

        self.time_var.set(values.get("SCHEDULE_TIME", "09:00"))

        expires_seconds = values.get("R2_PRESIGN_EXPIRES", "3600")
        try:
            minutes = max(1, int(int(expires_seconds) / 60))
        except ValueError:
            minutes = 60
        self.expiry_var.set(str(minutes))

        self._toggle_mode_fields()

    def _build_date_options(self, days: int) -> List[str]:
        today = dt.date.today()
        return [(today + dt.timedelta(days=i)).isoformat() for i in range(days + 1)]

    def _beijing_now(self) -> dt.datetime:
        return dt.datetime.now(dt.timezone(dt.timedelta(hours=8)))

    def _weekday_index(self, name: str) -> int:
        return WEEKDAYS.index(name)

    def _toggle_mode_fields(self) -> None:
        mode = self.mode_var.get().strip().lower()
        self.weekday_combo.configure(state="readonly" if mode == "weekly" else "disabled")
        self.date_combo.configure(state="readonly" if mode == "single" else "disabled")

    def _validate(self) -> Dict[str, str]:
        mode_text = self.mode_var.get().strip().lower()
        if mode_text not in ("single", "weekly"):
            raise ValueError("Trigger cycle must be single or weekly")

        time_text = self.time_var.get().strip()
        if not TIME_PATTERN.match(time_text):
            raise ValueError("Trigger time must be in HH:MM format, e.g. 09:00")

        minutes_text = self.expiry_var.get().strip()
        if not minutes_text.isdigit():
            raise ValueError("Link expiry must be a positive integer in minutes")
        minutes = int(minutes_text)
        if minutes < 1 or minutes > 1440:
            raise ValueError("Link expiry must be between 1 and 1440 minutes")

        updates = {
            "SCHEDULE_MODE": "date" if mode_text == "single" else "weekly",
            "SCHEDULE_TIME": time_text,
            "R2_PRESIGN_EXPIRES": str(minutes * 60),
            "R2_ENABLED": "true",
            "SCHEDULE_WEEKDAY": self.weekday_var.get().strip().lower() or "monday",
            "SCHEDULE_DATE": self.date_var.get().strip(),
        }

        if mode_text == "weekly":
            if updates["SCHEDULE_WEEKDAY"] not in WEEKDAYS:
                raise ValueError("Weekday must be monday..sunday")
        else:
            if updates["SCHEDULE_DATE"] not in self.date_options:
                raise ValueError("Specific date must be selected from dropdown options")
            if not DATE_PATTERN.match(updates["SCHEDULE_DATE"]):
                raise ValueError("Specific date format invalid")

        return updates

    def _compute_next_trigger(self, updates: Dict[str, str]) -> Optional[dt.datetime]:
        now = self._beijing_now()
        hour, minute = updates["SCHEDULE_TIME"].split(":")
        hour_i, minute_i = int(hour), int(minute)
        mode = updates["SCHEDULE_MODE"]

        if mode == "date":
            target_date = dt.datetime.strptime(updates["SCHEDULE_DATE"], "%Y-%m-%d").date()
            target = dt.datetime(
                target_date.year,
                target_date.month,
                target_date.day,
                hour_i,
                minute_i,
                0,
                tzinfo=dt.timezone(dt.timedelta(hours=8)),
            )
            return target if target > now else None

        target_weekday = self._weekday_index(updates["SCHEDULE_WEEKDAY"])
        days_ahead = target_weekday - now.weekday()
        if days_ahead < 0:
            days_ahead += 7
        target_date = (now + dt.timedelta(days=days_ahead)).date()
        target = dt.datetime(
            target_date.year,
            target_date.month,
            target_date.day,
            hour_i,
            minute_i,
            0,
            tzinfo=dt.timezone(dt.timedelta(hours=8)),
        )
        if target <= now:
            target += dt.timedelta(days=7)
        return target

    def _append_log(self, text: str) -> None:
        self.log_text.insert("end", text)
        self.log_text.see("end")

    def _poll_events(self) -> None:
        while True:
            try:
                event_type, payload = self.event_queue.get_nowait()
            except queue.Empty:
                break

            if event_type == "status":
                self.status_var.set(str(payload))
            elif event_type == "log":
                self._append_log(str(payload))
            elif event_type == "next_trigger":
                self.next_trigger_dt = payload
            elif event_type == "link":
                self.latest_link = str(payload)
                self.link_var.set(self.latest_link if self.latest_link else "-")
            elif event_type == "stopped":
                self.next_trigger_dt = None

        self.root.after(200, self._poll_events)

    def _refresh_countdown(self) -> None:
        if self.next_trigger_dt is None:
            self.countdown_var.set("Next trigger: -")
        else:
            now = self._beijing_now()
            remain = int((self.next_trigger_dt - now).total_seconds())
            if remain <= 0:
                self.countdown_var.set("Next trigger: executing...")
            else:
                hours, rem = divmod(remain, 3600)
                minutes, seconds = divmod(rem, 60)
                self.countdown_var.set(
                    f"Next trigger: {self.next_trigger_dt.strftime('%Y-%m-%d %H:%M')} (in {hours:02d}:{minutes:02d}:{seconds:02d})"
                )
        self.root.after(1000, self._refresh_countdown)

    def _run_scheduler_once_with_stream(self) -> None:
        cmd = [sys.executable, self.scheduler_path, "--mode", "once"]
        self.event_queue.put(("log", f"\n$ {' '.join(cmd)}\n"))
        output_buf: List[str] = []

        try:
            proc = subprocess.Popen(
                cmd,
                cwd=self.project_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            self.current_process = proc

            while True:
                if self.stop_event.is_set() and proc.poll() is None:
                    proc.terminate()
                chunk = proc.stdout.read(1) if proc.stdout else ""
                if chunk:
                    output_buf.append(chunk)
                    self.event_queue.put(("log", chunk))
                    continue
                if proc.poll() is not None:
                    break
                time.sleep(0.03)

            code = proc.wait()
            self.event_queue.put(("status", f"Run finished with exit code {code}"))
        except Exception as ex:
            self.event_queue.put(("status", f"Run failed: {ex}"))
            return
        finally:
            self.current_process = None

        full_output = "".join(output_buf)
        presigned = ""
        marker_matches = R2_LINK_PATTERN.findall(full_output)
        if marker_matches:
            presigned = marker_matches[-1]
        else:
            urls = URL_PATTERN.findall(full_output)
            for item in reversed(urls):
                if "X-Amz-Algorithm=" in item:
                    presigned = item
                    break
        if presigned:
            self.event_queue.put(("link", presigned))

    def _scheduler_worker(self, updates: Dict[str, str]) -> None:
        self.event_queue.put(("status", "Trigger started"))

        while not self.stop_event.is_set():
            next_dt = self._compute_next_trigger(updates)
            if next_dt is None:
                self.event_queue.put(("status", "Selected date has passed. Please choose a future date."))
                self.event_queue.put(("stopped", ""))
                return

            self.event_queue.put(("next_trigger", next_dt))
            while not self.stop_event.is_set() and self._beijing_now() < next_dt:
                time.sleep(1)

            if self.stop_event.is_set():
                self.event_queue.put(("status", "Trigger stopped"))
                self.event_queue.put(("stopped", ""))
                return

            self.event_queue.put(("status", "Trigger fired, running workflow..."))
            self._run_scheduler_once_with_stream()

            if updates["SCHEDULE_MODE"] == "date":
                self.event_queue.put(("status", "Single trigger completed"))
                self.event_queue.put(("stopped", ""))
                return

    def save_settings(self) -> None:
        try:
            updates = self._validate()
            upsert_env_file(self.env_path, updates)
            mode = updates["SCHEDULE_MODE"]
            if mode == "weekly":
                desc = f"weekly {updates['SCHEDULE_WEEKDAY']} {updates['SCHEDULE_TIME']}"
            else:
                desc = f"single {updates['SCHEDULE_DATE']} {updates['SCHEDULE_TIME']}"
            self.status_var.set(f"Saved: {desc} (Beijing), URL {self.expiry_var.get()} min")
            messagebox.showinfo("Saved", "Settings saved to .env")
        except Exception as ex:
            messagebox.showerror("Validation error", str(ex))

    def start_trigger(self) -> None:
        try:
            updates = self._validate()
            upsert_env_file(self.env_path, updates)
            if self.scheduler_thread and self.scheduler_thread.is_alive():
                messagebox.showwarning("Running", "Trigger is already running.")
                return

            self.stop_event.clear()
            self.scheduler_thread = threading.Thread(
                target=self._scheduler_worker,
                args=(updates,),
                daemon=True,
            )
            self.scheduler_thread.start()
            messagebox.showinfo("Started", f"{self.mode_var.get()} trigger started.")
        except Exception as ex:
            messagebox.showerror("Start failed", str(ex))

    def stop_trigger(self) -> None:
        self.stop_event.set()
        self.status_var.set("Stopping trigger...")
        if self.current_process and self.current_process.poll() is None:
            self.current_process.terminate()

    def run_once_now(self) -> None:
        try:
            updates = self._validate()
            upsert_env_file(self.env_path, updates)
            if self.scheduler_thread and self.scheduler_thread.is_alive():
                messagebox.showwarning("Running", "Please stop current trigger first.")
                return

            self.stop_event.clear()
            self.scheduler_thread = threading.Thread(
                target=self._run_scheduler_once_with_stream,
                daemon=True,
            )
            self.scheduler_thread.start()
            self.status_var.set("Triggered one immediate run.")
            messagebox.showinfo("Triggered", "One run was triggered immediately.")
        except Exception as ex:
            messagebox.showerror("Run failed", str(ex))

    def open_latest_link(self, _event=None) -> None:
        if self.latest_link:
            webbrowser.open(self.latest_link)


def main() -> int:
    project_dir = os.path.dirname(os.path.abspath(__file__))
    root = tk.Tk()
    root.configure(background="#f6f8fb")
    SettingsApp(root, project_dir=project_dir)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
