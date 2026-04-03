#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import time

from common import alert, connect_db, init_db, load_config, log_event, rotate_binding_session, set_message_state


def age_seconds(iso_value: str | None) -> float:
    if not iso_value:
        return 0.0
    return (dt.datetime.now(dt.timezone.utc) - dt.datetime.fromisoformat(iso_value)).total_seconds()


def main() -> int:
    config = load_config()
    conn = connect_db()
    init_db(conn)
    print("line-bridge watcher started", flush=True)
    queue_timeout = int(config["queue_timeout_sec"])
    run_timeout = int(config["run_timeout_sec"])
    delivery_timeout = int(config["delivery_timeout_sec"])
    watch_interval = int(config["watch_interval_sec"])
    max_run_retries = int(config["max_run_retries"])

    while True:
        queued = conn.execute(
            "SELECT id, created_at, retry_count FROM messages WHERE state = 'queued' AND manual_hold = 0"
        ).fetchall()
        for row in queued:
            if age_seconds(row["created_at"]) > queue_timeout and int(row["retry_count"]) < max_run_retries:
                set_message_state(conn, row["id"], "received", "watcher", "requeue after queued timeout")
                log_event(conn, row["id"], "retry", "watcher", "queued timeout")

        running = conn.execute(
            "SELECT id, session_key, started_at, retry_count FROM messages WHERE state IN ('running', 'assistant_started') AND manual_hold = 0"
        ).fetchall()
        for row in running:
            if age_seconds(row["started_at"]) > run_timeout:
                if int(row["retry_count"]) < max_run_retries:
                    if int(row["retry_count"]) >= 1:
                        new_session_id = rotate_binding_session(conn, row["session_key"], "run timeout", config)
                        if new_session_id:
                            conn.execute(
                                "UPDATE messages SET binding_session_id = ?, updated_at = ? WHERE id = ?",
                                (new_session_id, dt.datetime.now(dt.timezone.utc).isoformat(), row["id"]),
                            )
                            conn.commit()
                    set_message_state(conn, row["id"], "run_failed", "watcher", "run timeout")
                    log_event(conn, row["id"], "retry", "watcher", "run timeout; runner may retry")
                else:
                    set_message_state(conn, row["id"], "abandoned", "watcher", "run timeout and retry budget exhausted")
                    alert(f"message abandoned after run timeout: {row['id']}")

        pending = conn.execute(
            """
            SELECT m.id, m.updated_at
            FROM messages m
            WHERE m.state = 'delivery_pending' AND m.manual_hold = 0
            """
        ).fetchall()
        for row in pending:
            if age_seconds(row["updated_at"]) > delivery_timeout:
                log_event(conn, row["id"], "retry", "watcher", "delivery pending timeout; sender will retry")

        failed = conn.execute(
            """
            SELECT m.id, m.session_key, m.retry_count, m.state
            FROM messages m
            WHERE m.state IN ('run_failed', 'delivery_failed') AND m.manual_hold = 0
            """
        ).fetchall()
        for row in failed:
            if row["state"] == "delivery_failed":
                delivery = conn.execute(
                    """
                    SELECT id, attempt_no, delivery_mode, error_text
                    FROM deliveries
                    WHERE message_id = ?
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (row["id"],),
                ).fetchone()
                if not delivery:
                    set_message_state(conn, row["id"], "abandoned", "watcher", "delivery failed with no delivery row")
                    alert(f"delivery abandoned without row: {row['id']}")
                    continue
                if int(delivery["attempt_no"]) >= int(config["max_delivery_retries"]):
                    set_message_state(conn, row["id"], "abandoned", "watcher", "max delivery retries reached")
                    alert(f"delivery abandoned after retries: {row['id']}")
                    continue
                conn.execute(
                    "UPDATE deliveries SET state = 'pending', updated_at = ? WHERE id = ?",
                    (dt.datetime.now(dt.timezone.utc).isoformat(), delivery["id"]),
                )
                conn.commit()
                set_message_state(conn, row["id"], "delivery_pending", "watcher", "retrying delivery only")
                log_event(conn, row["id"], "retry", "watcher", "delivery requeued")
                continue

            if int(row["retry_count"]) >= max_run_retries:
                set_message_state(conn, row["id"], "abandoned", "watcher", "max retries reached")
                alert(f"run abandoned after retries: {row['id']}")
            else:
                if int(row["retry_count"]) >= 1:
                    new_session_id = rotate_binding_session(conn, row["session_key"], "run failed retry", config)
                    if new_session_id:
                        conn.execute(
                            "UPDATE messages SET binding_session_id = ?, updated_at = ? WHERE id = ?",
                            (new_session_id, dt.datetime.now(dt.timezone.utc).isoformat(), row["id"]),
                        )
                        conn.commit()
                set_message_state(conn, row["id"], "queued", "watcher", "retrying failed message")
                log_event(conn, row["id"], "retry", "watcher", "requeued failed message")

        time.sleep(watch_interval)


if __name__ == "__main__":
    raise SystemExit(main())
