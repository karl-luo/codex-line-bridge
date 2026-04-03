#!/usr/bin/env python3
from __future__ import annotations

import json
import mimetypes
import pathlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from common import (
    CONFIG_PATH,
    compute_signature,
    connect_db,
    detect_kind_from_path,
    ensure_binding,
    get_line_group_require_mention,
    init_db,
    is_line_group_allowed,
    is_line_user_allowed,
    line_binary_request,
    line_request,
    load_config,
    log_event,
    new_id,
    parse_line_source,
    queue_line_group_pairing_request,
    queue_line_pairing_request,
    register_artifact,
    session_key_for,
    utcnow,
    write_json,
)


CONFIG = load_config()
CONN = connect_db()
init_db(CONN)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path == CONFIG["webhook_path"]:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"ok")
            return
        media_prefix = "/" + (CONFIG.get("media_path_prefix") or "/line/media").strip("/")
        if self.path.startswith(media_prefix + "/"):
            self.serve_artifact(media_prefix)
            return
        self.send_error(404)

    def do_POST(self) -> None:  # noqa: N802
        if self.path != CONFIG["webhook_path"]:
            self.send_error(404)
            return
        self.maybe_update_public_base_url()
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        signature = self.headers.get("X-Line-Signature", "")
        expected = compute_signature(CONFIG["line_channel_secret"], body)
        if not signature or signature != expected:
            self.send_error(401, "invalid signature")
            return

        payload = json.loads(body.decode("utf-8") or "{}")
        events = payload.get("events", [])
        for event in events:
            if event.get("type") != "message":
                continue
            message = event.get("message", {})
            if message.get("type") not in {"text", "image", "file"}:
                continue
            chat_type, chat_id, user_id = parse_line_source(event.get("source", {}))
            if not chat_id:
                continue
            if chat_type == "direct" and CONFIG.get("line_dm_policy") == "pairing" and not is_line_user_allowed(user_id):
                preview = str(message.get("text") or f"[{message.get('type', 'message')}]")
                req = queue_line_pairing_request(user_id or "", chat_id, preview)
                self.reply_unauthorized(event.get("replyToken"), chat_id)
                from common import alert
                alert(
                    "new LINE pairing request\n"
                    f"user_id: {user_id}\n"
                    f"chat_id: {chat_id}\n"
                    f"text: {preview}\n"
                    f"count: {req.get('requestCount', 1)}"
                )
                continue
            if chat_type in {"group", "room"} and CONFIG.get("line_group_policy") == "pairing" and not is_line_group_allowed(chat_id):
                preview = str(message.get("text") or f"[{message.get('type', 'message')}]")
                req = queue_line_group_pairing_request(chat_type, chat_id, user_id, preview)
                self.reply_unauthorized(event.get("replyToken"), chat_id, group_mode=True)
                from common import alert
                alert(
                    "new LINE group pairing request\n"
                    f"chat_type: {chat_type}\n"
                    f"chat_id: {chat_id}\n"
                    f"user_id: {user_id}\n"
                    f"text: {preview}\n"
                    f"count: {req.get('requestCount', 1)}"
                )
                continue
            if chat_type in {"group", "room"} and get_line_group_require_mention(
                chat_id, bool(CONFIG.get("line_group_require_mention_default", True))
            ) and not self.is_bot_mentioned(message):
                continue
            session_key = session_key_for(chat_type, chat_id)
            binding = ensure_binding(CONN, session_key, chat_type, chat_id, CONFIG)
            dedup_key = f"line:{chat_id}:{message.get('id')}"
            exists = CONN.execute("SELECT id FROM messages WHERE dedup_key = ?", (dedup_key,)).fetchone()
            if exists:
                continue
            msg_id = new_id()
            now = utcnow()
            text_content = message.get("text", "")
            if message.get("type") == "image":
                text_content = "[User sent an image]"
            elif message.get("type") == "file":
                text_content = f"[User sent a file: {message.get('fileName') or message.get('id')}]"
            CONN.execute(
                """
                INSERT INTO messages (
                  id, platform, chat_type, chat_id, user_id, line_message_id, line_reply_token,
                  text_content, raw_event_json, dedup_key, session_key, binding_session_id,
                  state, created_at, updated_at
                ) VALUES (?, 'line', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'received', ?, ?)
                """,
                (
                    msg_id,
                    chat_type,
                    chat_id,
                    user_id,
                    message.get("id"),
                    event.get("replyToken"),
                    text_content,
                    json.dumps(event, ensure_ascii=False),
                    dedup_key,
                    session_key,
                    binding["codex_session_id"],
                    now,
                    now,
                ),
            )
            CONN.commit()
            log_event(CONN, msg_id, "state_change", "webhook", "received from LINE", None, "received")
            log_event(CONN, msg_id, "binding", "webhook", f"bound to {binding['codex_session_id']}")
            if message.get("type") in {"image", "file"} and message.get("id"):
                self.store_line_media(msg_id, message)
            CONN.execute(
                "UPDATE messages SET state = 'queued', updated_at = ? WHERE id = ?",
                (utcnow(), msg_id),
            )
            CONN.commit()
            log_event(CONN, msg_id, "state_change", "webhook", "queued for runner", "received", "queued")

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return

    def maybe_update_public_base_url(self) -> None:
        host = self.headers.get("X-Forwarded-Host") or self.headers.get("Host") or ""
        proto = self.headers.get("X-Forwarded-Proto") or "https"
        if not host:
            return
        if host.startswith("127.0.0.1") or host.startswith("localhost"):
            return
        public_base = f"{proto}://{host}"
        if CONFIG.get("public_base_url") == public_base:
            return
        CONFIG["public_base_url"] = public_base
        write_json(CONFIG_PATH, CONFIG)

    def serve_artifact(self, media_prefix: str) -> None:
        parts = self.path[len(media_prefix) + 1 :].split("/", 1)
        if len(parts) != 2:
            self.send_error(404)
            return
        artifact_id, _ = parts
        row = CONN.execute(
            "SELECT local_path, mime_type FROM artifacts WHERE id = ?",
            (artifact_id,),
        ).fetchone()
        if not row:
            self.send_error(404)
            return
        path = pathlib.Path(row["local_path"])
        if not path.exists():
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", row["mime_type"] or mimetypes.guess_type(path.name)[0] or "application/octet-stream")
        self.send_header("Content-Length", str(path.stat().st_size))
        self.send_header("Cache-Control", "public, max-age=86400")
        self.end_headers()
        self.wfile.write(path.read_bytes())

    def store_line_media(self, msg_id: str, message: dict[str, object]) -> None:
        status, data, headers = line_binary_request(CONFIG, f"/v2/bot/message/{message.get('id')}/content")
        if status < 200 or status >= 300:
            log_event(CONN, msg_id, "error", "webhook", f"failed to fetch LINE media content: {status}")
            return
        content_type = headers.get("Content-Type", "application/octet-stream")
        filename = str(message.get("fileName") or message.get("id") or "file")
        ext = pathlib.Path(filename).suffix
        if not ext:
            guessed = mimetypes.guess_extension(content_type.split(";")[0].strip()) or ""
            filename = filename + guessed
        temp_dir = pathlib.Path("/tmp/line-bridge-inbound")
        temp_dir.mkdir(parents=True, exist_ok=True)
        temp_path = temp_dir / f"{msg_id}_{filename}"
        temp_path.write_bytes(data)
        kind = "image" if message.get("type") == "image" else detect_kind_from_path(str(temp_path))
        register_artifact(CONN, msg_id, None, str(temp_path), kind=kind, mime_type=content_type.split(";")[0].strip())
        log_event(CONN, msg_id, "artifact", "webhook", f"downloaded inbound {kind}: {filename}")

    def reply_unauthorized(self, reply_token: str | None, chat_id: str, group_mode: bool = False) -> None:
        text = CONFIG["unauthorized_group_reply_text"] if group_mode else CONFIG["unauthorized_reply_text"]
        payload = {"messages": [{"type": "text", "text": text[:5000]}]}
        if reply_token:
            payload["replyToken"] = reply_token
            status, data = line_request(CONFIG, "/v2/bot/message/reply", payload)
            if 200 <= status < 300:
                return
        line_request(
            CONFIG,
            "/v2/bot/message/push",
            {"to": chat_id, "messages": [{"type": "text", "text": text[:5000]}]},
        )

    def is_bot_mentioned(self, message: dict[str, object]) -> bool:
        mention = message.get("mention") or {}
        mentionees = mention.get("mentionees") or []
        if not isinstance(mentionees, list):
            return False
        return any(bool(item.get("isSelf")) for item in mentionees if isinstance(item, dict))


def main() -> int:
    host = CONFIG["webhook_bind_host"]
    port = int(CONFIG["webhook_port"])
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"LINE webhook listening on http://{host}:{port}{CONFIG['webhook_path']}")
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
