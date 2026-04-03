#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
from typing import Iterable

from common import DB_PATH, approve_line_user, load_line_allow_from, load_line_pairing, reject_line_user


LABELS = [
    "ai.codex.line-bridge-webhook",
    "ai.codex.line-bridge-runner",
    "ai.codex.line-bridge-sender",
    "ai.codex.line-bridge-watcher",
]


def run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, text=True, capture_output=True)


def uid() -> str:
    return str(os.getuid())


def launchctl(*args: str) -> subprocess.CompletedProcess[str]:
    return run(["launchctl", *args])


def status() -> int:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    counts = {}
    for state, c in conn.execute("select state, count(*) as c from messages group by state"):
        counts[state] = c
    latest = [
        dict(r) for r in conn.execute(
            "select id, chat_id, text_content, state, retry_count, last_error, updated_at from messages order by updated_at desc limit 5"
        )
    ]
    service_lines = []
    for label in LABELS:
        out = launchctl("print", f"gui/{uid()}/{label}")
        service_lines.append({
            "label": label,
            "ok": out.returncode == 0,
        })
    print(json.dumps({"services": service_lines, "message_counts": counts, "latest_messages": latest}, ensure_ascii=False, indent=2))
    return 0


def start() -> int:
    for label in LABELS:
        plist = os.path.expanduser(f"~/Library/LaunchAgents/{label}.plist")
        launchctl("bootstrap", f"gui/{uid()}", plist)
        launchctl("enable", f"gui/{uid()}/{label}")
        launchctl("kickstart", "-k", f"gui/{uid()}/{label}")
    return status()


def stop() -> int:
    for label in LABELS:
        launchctl("bootout", f"gui/{uid()}/{label}")
        launchctl("disable", f"gui/{uid()}/{label}")
    return 0


def retry(message_id: str) -> int:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "update messages set state = 'queued', last_error = null, updated_at = datetime('now') where id = ?",
        (message_id,),
    )
    conn.commit()
    print(f"requeued {message_id}")
    return 0


def inspect(message_id: str) -> int:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    msg = conn.execute("select * from messages where id = ?", (message_id,)).fetchone()
    if not msg:
        print("message not found")
        return 1
    runs = [dict(r) for r in conn.execute("select * from runs where message_id = ? order by created_at desc", (message_id,))]
    deliveries = [dict(r) for r in conn.execute("select * from deliveries where message_id = ? order by created_at desc", (message_id,))]
    events = [dict(r) for r in conn.execute("select * from message_events where message_id = ? order by created_at desc", (message_id,))]
    print(json.dumps({"message": dict(msg), "runs": runs, "deliveries": deliveries, "events": events}, ensure_ascii=False, indent=2))
    return 0


def rotate_session(message_id: str) -> int:
    from common import load_config, rotate_binding_session
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    msg = conn.execute("select id, session_key from messages where id = ?", (message_id,)).fetchone()
    if not msg:
        print("message not found")
        return 1
    new_session_id = rotate_binding_session(conn, msg["session_key"], "manual rotate", load_config())
    if new_session_id:
        conn.execute("update messages set binding_session_id = ? where id = ?", (new_session_id, message_id))
        conn.commit()
    print(json.dumps({"message_id": message_id, "new_session_id": new_session_id}, ensure_ascii=False, indent=2))
    return 0


def pairings() -> int:
    data = load_line_pairing()
    allow = load_line_allow_from()
    print(json.dumps({"pending_requests": data.get("requests", []), "allow_from": allow.get("allowFrom", [])}, ensure_ascii=False, indent=2))
    return 0


def approve_user(user_id: str) -> int:
    approve_line_user(user_id)
    print(json.dumps({"approved": user_id}, ensure_ascii=False, indent=2))
    return 0


def reject_user(user_id: str) -> int:
    reject_line_user(user_id)
    print(json.dumps({"rejected": user_id}, ensure_ascii=False, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status")
    sub.add_parser("start")
    sub.add_parser("stop")
    p_retry = sub.add_parser("retry")
    p_retry.add_argument("message_id")
    p_inspect = sub.add_parser("inspect")
    p_inspect.add_argument("message_id")
    p_rotate = sub.add_parser("rotate-session")
    p_rotate.add_argument("message_id")
    sub.add_parser("pairings")
    p_approve = sub.add_parser("approve-user")
    p_approve.add_argument("user_id")
    p_reject = sub.add_parser("reject-user")
    p_reject.add_argument("user_id")
    args = parser.parse_args()

    if args.cmd == "status":
        return status()
    if args.cmd == "start":
        return start()
    if args.cmd == "stop":
        return stop()
    if args.cmd == "retry":
        return retry(args.message_id)
    if args.cmd == "inspect":
        return inspect(args.message_id)
    if args.cmd == "rotate-session":
        return rotate_session(args.message_id)
    if args.cmd == "pairings":
        return pairings()
    if args.cmd == "approve-user":
        return approve_user(args.user_id)
    if args.cmd == "reject-user":
        return reject_user(args.user_id)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
