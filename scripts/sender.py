#!/usr/bin/env python3
from __future__ import annotations

import json
import time

from common import (
    artifact_public_url,
    connect_db,
    init_db,
    line_request,
    load_config,
    log_event,
    set_message_state,
    utcnow,
)


def build_messages(conn, config, row) -> list[dict[str, str]]:
    try:
        payload = json.loads(row["payload_json"])
    except Exception:
        payload = {"text": row["payload_json"], "artifact_ids": []}

    text = str(payload.get("text") or "").strip()
    artifact_ids = list(payload.get("artifact_ids") or [])
    messages: list[dict[str, str]] = []

    if text:
        messages.append({"type": "text", "text": text[:5000]})

    for artifact_id in artifact_ids:
        artifact = conn.execute(
            "SELECT id, kind, local_path FROM artifacts WHERE id = ?",
            (artifact_id,),
        ).fetchone()
        if not artifact:
            continue
        url = artifact_public_url(config, artifact["id"], artifact["local_path"].split("/")[-1])
        if artifact["kind"] == "image" and url:
            messages.append(
                {
                    "type": "image",
                    "originalContentUrl": url,
                    "previewImageUrl": url,
                }
            )
            continue
        if artifact["kind"] in {"pdf", "file"} and url:
            messages.append({"type": "text", "text": f"{artifact['local_path'].split('/')[-1]}\n{url}"[:5000]})
            continue
        messages.append({"type": "text", "text": f"Saved artifact: {artifact['local_path']}"[:5000]})

    if not messages:
        messages.append({"type": "text", "text": "(empty reply)"})
    return messages[:5]


def main() -> int:
    config = load_config()
    conn = connect_db()
    init_db(conn)
    print("line-bridge sender started", flush=True)

    while True:
        row = conn.execute(
            """
            SELECT d.*, m.chat_id, m.line_reply_token, m.id AS message_pk
            FROM deliveries d
            JOIN messages m ON m.id = d.message_id
            WHERE d.state = 'pending'
            ORDER BY d.created_at ASC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            time.sleep(2)
            continue

        messages = build_messages(conn, config, row)
        if row["delivery_mode"] == "reply" and row["line_reply_token"]:
            status, data = line_request(
                config,
                "/v2/bot/message/reply",
                {
                    "replyToken": row["line_reply_token"],
                    "messages": messages,
                },
            )
            if status >= 400 and "reply token" in json.dumps(data, ensure_ascii=False).lower():
                conn.execute(
                    "UPDATE deliveries SET delivery_mode = 'push', updated_at = ? WHERE id = ?",
                    (utcnow(), row["id"]),
                )
                conn.commit()
                log_event(conn, row["message_id"], "retry", "sender", "reply token expired, switching to push")
                continue
        else:
            status, data = line_request(
                config,
                "/v2/bot/message/push",
                {
                    "to": row["chat_id"],
                    "messages": messages,
                },
            )

        if 200 <= status < 300:
            conn.execute(
                "UPDATE deliveries SET state = 'sent', updated_at = ?, sent_at = ? WHERE id = ?",
                (utcnow(), utcnow(), row["id"]),
            )
            conn.commit()
            set_message_state(conn, row["message_id"], "delivered", "sender", "delivery confirmed")
        else:
            conn.execute(
                """
                UPDATE deliveries
                SET state = 'failed', attempt_no = attempt_no + 1, error_text = ?, updated_at = ?
                WHERE id = ?
                """,
                (json.dumps(data, ensure_ascii=False)[:4000], utcnow(), row["id"]),
            )
            conn.execute(
                "UPDATE messages SET last_error = ?, updated_at = ? WHERE id = ?",
                (json.dumps(data, ensure_ascii=False)[:4000], utcnow(), row["message_id"]),
            )
            conn.commit()
            set_message_state(conn, row["message_id"], "delivery_failed", "sender", f"LINE API {status}")
            log_event(conn, row["message_id"], "error", "sender", json.dumps(data, ensure_ascii=False)[:4000])
            time.sleep(1)


if __name__ == "__main__":
    raise SystemExit(main())
