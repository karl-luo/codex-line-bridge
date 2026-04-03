#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
import re
import time
import uuid

from PIL import Image

from common import (
    connect_db,
    conversation_prompt,
    extract_local_paths,
    init_db,
    load_config,
    log_event,
    new_id,
    register_artifact,
    run_codex,
    set_message_state,
    strip_local_paths,
    utcnow,
)


def is_resend_request(text: str) -> bool:
    if is_pdf_merge_request(text):
        return False
    return bool(re.search("(\u518d\u53d1|\u91cd\u53d1|\u518d\u4f20|\u91cd\u65b0\u53d1|\u53d1\u7ed9\u6211|\u628a\u521a\u624d.+\u53d1\u7ed9\u6211|\u628a.+\u56fe\u7247.+\u53d1\u7ed9\u6211)", text))


def wants_multiple_artifacts(text: str) -> bool:
    return bool(re.search("(\u90fd\u53d1|\u5168\u90e8\u53d1|\u4e00\u8d77\u53d1|\u8fd9\u51e0\u5f20|\u6240\u6709\u56fe\u7247|\u5168\u90e8\u56fe\u7247)", text))


def is_pdf_merge_request(text: str) -> bool:
    return bool(re.search("(\u5408\u5e76|\u62fc\u6210|\u505a\u6210).*(pdf|PDF)|(pdf|PDF).*(\u5408\u5e76|\u53d1\u7ed9\u6211)", text))


def recent_session_artifact_ids(conn, session_key: str, limit: int = 1) -> list[str]:
    rows = conn.execute(
        """
        SELECT a.id
        FROM artifacts a
        JOIN messages m ON m.id = a.message_id
        WHERE m.session_key = ?
        ORDER BY a.created_at DESC
        LIMIT ?
        """,
        (session_key, limit),
    ).fetchall()
    return [row["id"] for row in rows]


def recent_session_image_paths(conn, session_key: str, limit: int = 2) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT a.local_path
        FROM artifacts a
        JOIN messages m ON m.id = a.message_id
        WHERE m.session_key = ? AND a.kind = 'image'
        ORDER BY a.created_at DESC
        LIMIT ?
        """,
        (session_key, limit),
    ).fetchall()
    return [row["local_path"] for row in reversed(rows)]


def build_pdf_from_images(image_paths: list[str], message_id: str) -> str | None:
    if len(image_paths) < 2:
        return None
    out_dir = Path(__file__).resolve().parents[1] / "runtime" / "generated"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{message_id}-{uuid.uuid4().hex[:8]}.pdf"
    images = []
    try:
        for path in image_paths:
            img = Image.open(path)
            if img.mode != "RGB":
                img = img.convert("RGB")
            images.append(img)
        images[0].save(out_path, "PDF", resolution=150.0, save_all=True, append_images=images[1:])
        return str(out_path)
    finally:
        for img in images:
            try:
                img.close()
            except Exception:
                pass


def main() -> int:
    config = load_config()
    conn = connect_db()
    init_db(conn)
    print("line-bridge runner started", flush=True)

    while True:
        row = conn.execute(
            """
            SELECT * FROM messages
            WHERE state IN ('received', 'bound', 'queued')
              AND manual_hold = 0
              AND retry_count < ?
            ORDER BY created_at ASC
            LIMIT 1
            """,
            (int(config["max_run_retries"]),),
        ).fetchone()
        if not row:
            time.sleep(2)
            continue

        message_id = row["id"]
        session_key = row["session_key"]
        attempt_no = int(row["retry_count"]) + 1
        run_id = new_id()

        conn.execute(
            "UPDATE messages SET retry_count = ?, started_at = ?, updated_at = ? WHERE id = ?",
            (attempt_no, utcnow(), utcnow(), message_id),
        )
        conn.execute(
            """
            INSERT INTO runs (
              id, message_id, binding_session_id, codex_session_id, attempt_no,
              trigger_source, state, started_at, created_at
            ) VALUES (?, ?, ?, ?, ?, 'initial', 'running', ?, ?)
            """,
            (run_id, message_id, row["binding_session_id"], row["binding_session_id"], attempt_no, utcnow(), utcnow()),
        )
        conn.commit()

        set_message_state(conn, message_id, "queued", "runner", "picked by runner")
        set_message_state(conn, message_id, "running", "runner", "starting codex run")
        set_message_state(conn, message_id, "assistant_started", "runner", "codex run started")

        if is_pdf_merge_request(row["text_content"] or ""):
            pdf_path = build_pdf_from_images(recent_session_image_paths(conn, session_key, limit=2), message_id)
            if pdf_path:
                artifact_id = register_artifact(conn, message_id, run_id, pdf_path, kind="pdf", mime_type="application/pdf")
                conn.execute(
                    "UPDATE runs SET state = 'completed', finished_at = ?, exit_code = 0 WHERE id = ?",
                    (utcnow(), run_id),
                )
                conn.execute(
                    """
                    INSERT INTO deliveries (
                      id, message_id, run_id, channel, delivery_mode, target_id,
                      payload_json, state, created_at, updated_at
                    ) VALUES (?, ?, ?, 'line', 'reply', ?, ?, 'pending', ?, ?)
                    """,
                    (
                        new_id(),
                        message_id,
                        run_id,
                        row["chat_id"],
                        json.dumps({"text": "Merged into one PDF.", "artifact_ids": [artifact_id]}, ensure_ascii=False),
                        utcnow(),
                        utcnow(),
                    ),
                )
                conn.commit()
                set_message_state(conn, message_id, "assistant_completed", "runner", "pdf generated directly")
                set_message_state(conn, message_id, "delivery_pending", "runner", "queued pdf for sender")
                log_event(conn, message_id, "note", "runner", "generated pdf from recent images")
                time.sleep(1)
                continue

        prompt = conversation_prompt(conn, session_key, message_id)
        code, reply, stderr = run_codex(prompt, config)

        if code == 0 and reply.strip():
            artifact_ids: list[str] = []
            for path in extract_local_paths(reply):
                try:
                    artifact_ids.append(register_artifact(conn, message_id, run_id, path))
                except Exception as exc:
                    log_event(conn, message_id, "error", "runner", f"artifact registration failed for {path}: {exc}")
            if not artifact_ids and is_resend_request(row["text_content"] or ""):
                fallback_limit = 3 if wants_multiple_artifacts(row["text_content"] or "") else 1
                artifact_ids = recent_session_artifact_ids(conn, session_key, limit=fallback_limit)
            cleaned_reply = strip_local_paths(reply)
            conn.execute(
                "UPDATE runs SET state = 'completed', finished_at = ?, exit_code = 0 WHERE id = ?",
                (utcnow(), run_id),
            )
            conn.execute(
                """
                INSERT INTO deliveries (
                  id, message_id, run_id, channel, delivery_mode, target_id,
                  payload_json, state, created_at, updated_at
                ) VALUES (?, ?, ?, 'line', 'reply', ?, ?, 'pending', ?, ?)
                """,
                (
                    new_id(),
                    message_id,
                    run_id,
                    row["chat_id"],
                    json.dumps({"text": cleaned_reply, "artifact_ids": artifact_ids}, ensure_ascii=False),
                    utcnow(),
                    utcnow(),
                ),
            )
            conn.commit()
            set_message_state(conn, message_id, "assistant_completed", "runner", "reply generated")
            set_message_state(conn, message_id, "delivery_pending", "runner", "queued for sender")
            log_event(conn, message_id, "note", "runner", "codex reply generated successfully")
        else:
            conn.execute(
                "UPDATE runs SET state = 'failed', finished_at = ?, exit_code = ?, error_text = ? WHERE id = ?",
                (utcnow(), code, stderr[:4000], run_id),
            )
            conn.execute(
                "UPDATE messages SET last_error = ?, updated_at = ? WHERE id = ?",
                ((stderr or "empty reply")[:4000], utcnow(), message_id),
            )
            conn.commit()
            set_message_state(conn, message_id, "run_failed", "runner", stderr[:500] or "empty reply")
            log_event(conn, message_id, "error", "runner", stderr[:4000] or "codex returned empty reply")
            time.sleep(1)


if __name__ == "__main__":
    raise SystemExit(main())
