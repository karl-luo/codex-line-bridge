from __future__ import annotations

import base64
import datetime as dt
import hashlib
import hmac
import json
import mimetypes
import os
import pathlib
import sqlite3
import subprocess
import tempfile
import uuid
import re
from typing import Any
from urllib import error, request


SKILL_DIR = pathlib.Path(__file__).resolve().parents[1]
DATA_DIR = SKILL_DIR / "data"
LOG_DIR = SKILL_DIR / "logs"
RUNTIME_DIR = SKILL_DIR / "runtime"
ARTIFACTS_DIR = RUNTIME_DIR / "artifacts"
DB_PATH = DATA_DIR / "line_bridge.sqlite3"
CONFIG_PATH = DATA_DIR / "config.json"
OPENCLAW_HOME = pathlib.Path(os.environ.get("OPENCLAW_HOME", str(pathlib.Path.home() / ".openclaw")))
OPENCLAW_CONFIG_PATH = OPENCLAW_HOME / "openclaw.json"
DEFAULT_WORKDIR = os.environ.get("CODEX_WORKDIR", os.getcwd())
CODEX_BIN = os.environ.get("CODEX_BIN", "codex")
ALERT_LOG_PATH = LOG_DIR / "alerts.log"
OPENCLAW_LINE_ALLOW_FROM_PATH = OPENCLAW_HOME / "credentials" / "line-allowFrom.json"
OPENCLAW_LINE_PAIRING_PATH = OPENCLAW_HOME / "credentials" / "line-pairing.json"
LOCAL_LINE_ALLOW_FROM_PATH = DATA_DIR / "line-allowFrom.json"
LOCAL_LINE_PAIRING_PATH = DATA_DIR / "line-pairing.json"
LOCAL_LINE_ALLOW_GROUPS_PATH = DATA_DIR / "line-allowGroups.json"
LOCAL_LINE_GROUP_PAIRING_PATH = DATA_DIR / "line-group-pairing.json"
LOCAL_LINE_GROUP_SETTINGS_PATH = DATA_DIR / "line-group-settings.json"


MESSAGE_STATES = {
    "received",
    "bound",
    "queued",
    "running",
    "assistant_started",
    "assistant_completed",
    "delivery_pending",
    "delivered",
    "run_failed",
    "delivery_failed",
    "abandoned",
}


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)


def utcnow() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def new_id() -> str:
    return str(uuid.uuid4())


def read_json(path: pathlib.Path, fallback: Any) -> Any:
    try:
        return json.loads(path.read_text())
    except Exception:
        return fallback


def write_json(path: pathlib.Path, data: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    tmp.replace(path)


def append_log(path: pathlib.Path, line: str) -> None:
    ensure_dirs()
    with path.open("a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")


def load_openclaw_line_config() -> dict[str, Any]:
    raw = read_json(OPENCLAW_CONFIG_PATH, {})
    return raw.get("channels", {}).get("line", {}) or {}


def load_cti_discord_token() -> str:
    path = pathlib.Path(os.environ.get("CTI_CONFIG_PATH", str(pathlib.Path.home() / ".claude-to-im" / "config.env")))
    if not path.exists():
        return ""
    text = path.read_text()
    m = re.search(r"^CTI_DISCORD_BOT_TOKEN=(.+)$", text, re.M)
    return m.group(1).strip() if m else ""


def default_config() -> dict[str, Any]:
    line = load_openclaw_line_config()
    openclaw = read_json(OPENCLAW_CONFIG_PATH, {})
    discord = openclaw.get("channels", {}).get("discord", {}) or {}
    discord_token = load_cti_discord_token() or discord.get("token", "")
    return {
        "line_channel_access_token": line.get("channelAccessToken", ""),
        "line_channel_secret": line.get("channelSecret", ""),
        "line_dm_policy": line.get("dmPolicy", "open"),
        "line_group_policy": "pairing",
        "line_group_require_mention_default": True,
        "discord_bot_token": discord_token,
        "webhook_bind_host": "127.0.0.1",
        "webhook_port": 8080,
        "webhook_path": "/line/webhook",
        "media_path_prefix": "/line/media",
        "public_base_url": "",
        "unauthorized_reply_text": "This LINE account is not approved yet. Your request has been recorded and is waiting for admin approval.",
        "unauthorized_group_reply_text": "This LINE group is not approved yet. The group has been queued for admin approval.",
        "codex_workdir": DEFAULT_WORKDIR,
        "codex_model": "",
        "session_mode": "hybrid",
        "reply_timeout_sec": 600,
        "queue_timeout_sec": 60,
        "run_timeout_sec": 900,
        "delivery_timeout_sec": 30,
        "max_run_retries": 3,
        "max_delivery_retries": 5,
        "watch_interval_sec": 20,
        "alarm_channel": "1479310669382815867",
    }


def load_config() -> dict[str, Any]:
    ensure_dirs()
    if not CONFIG_PATH.exists():
        write_json(CONFIG_PATH, default_config())
    cfg = read_json(CONFIG_PATH, {})
    merged = default_config()
    merged.update(cfg)
    if not merged.get("alarm_channel"):
        merged["alarm_channel"] = default_config()["alarm_channel"]
    return merged


def connect_db() -> sqlite3.Connection:
    ensure_dirs()
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def ensure_line_auth_files() -> None:
    if not LOCAL_LINE_ALLOW_FROM_PATH.exists():
        source = read_json(OPENCLAW_LINE_ALLOW_FROM_PATH, {"version": 1, "allowFrom": []})
        write_json(LOCAL_LINE_ALLOW_FROM_PATH, source)
    if not LOCAL_LINE_PAIRING_PATH.exists():
        source = read_json(OPENCLAW_LINE_PAIRING_PATH, {"version": 1, "requests": []})
        write_json(LOCAL_LINE_PAIRING_PATH, source)
    if not LOCAL_LINE_ALLOW_GROUPS_PATH.exists():
        write_json(LOCAL_LINE_ALLOW_GROUPS_PATH, {"version": 1, "allowGroups": []})
    if not LOCAL_LINE_GROUP_PAIRING_PATH.exists():
        write_json(LOCAL_LINE_GROUP_PAIRING_PATH, {"version": 1, "requests": []})
    if not LOCAL_LINE_GROUP_SETTINGS_PATH.exists():
        write_json(LOCAL_LINE_GROUP_SETTINGS_PATH, {"version": 1, "groups": {}})


def load_line_allow_from() -> dict[str, Any]:
    ensure_line_auth_files()
    raw = read_json(LOCAL_LINE_ALLOW_FROM_PATH, {"version": 1, "allowFrom": []})
    raw.setdefault("version", 1)
    raw.setdefault("allowFrom", [])
    return raw


def save_line_allow_from(data: dict[str, Any]) -> None:
    write_json(LOCAL_LINE_ALLOW_FROM_PATH, data)


def load_line_pairing() -> dict[str, Any]:
    ensure_line_auth_files()
    raw = read_json(LOCAL_LINE_PAIRING_PATH, {"version": 1, "requests": []})
    raw.setdefault("version", 1)
    raw.setdefault("requests", [])
    return raw


def save_line_pairing(data: dict[str, Any]) -> None:
    write_json(LOCAL_LINE_PAIRING_PATH, data)


def is_line_user_allowed(user_id: str | None) -> bool:
    if not user_id:
        return False
    data = load_line_allow_from()
    return user_id in set(data.get("allowFrom", []))


def queue_line_pairing_request(user_id: str, chat_id: str, text_preview: str) -> dict[str, Any]:
    data = load_line_pairing()
    now = utcnow()
    requests = data.get("requests", [])
    for item in requests:
        if item.get("userId") == user_id:
            item["chatId"] = chat_id
            item["lastSeenAt"] = now
            item["lastTextPreview"] = text_preview[:500]
            item["requestCount"] = int(item.get("requestCount", 1)) + 1
            save_line_pairing(data)
            return item
    item = {
        "userId": user_id,
        "chatId": chat_id,
        "firstSeenAt": now,
        "lastSeenAt": now,
        "lastTextPreview": text_preview[:500],
        "requestCount": 1,
    }
    requests.append(item)
    save_line_pairing(data)
    return item


def approve_line_user(user_id: str) -> None:
    allow = load_line_allow_from()
    if user_id not in allow["allowFrom"]:
        allow["allowFrom"].append(user_id)
        save_line_allow_from(allow)
    pairing = load_line_pairing()
    pairing["requests"] = [item for item in pairing.get("requests", []) if item.get("userId") != user_id]
    save_line_pairing(pairing)


def reject_line_user(user_id: str) -> None:
    pairing = load_line_pairing()
    pairing["requests"] = [item for item in pairing.get("requests", []) if item.get("userId") != user_id]
    save_line_pairing(pairing)


def load_line_allow_groups() -> dict[str, Any]:
    ensure_line_auth_files()
    raw = read_json(LOCAL_LINE_ALLOW_GROUPS_PATH, {"version": 1, "allowGroups": []})
    raw.setdefault("version", 1)
    raw.setdefault("allowGroups", [])
    return raw


def save_line_allow_groups(data: dict[str, Any]) -> None:
    write_json(LOCAL_LINE_ALLOW_GROUPS_PATH, data)


def load_line_group_pairing() -> dict[str, Any]:
    ensure_line_auth_files()
    raw = read_json(LOCAL_LINE_GROUP_PAIRING_PATH, {"version": 1, "requests": []})
    raw.setdefault("version", 1)
    raw.setdefault("requests", [])
    return raw


def save_line_group_pairing(data: dict[str, Any]) -> None:
    write_json(LOCAL_LINE_GROUP_PAIRING_PATH, data)


def is_line_group_allowed(chat_id: str | None) -> bool:
    if not chat_id:
        return False
    data = load_line_allow_groups()
    return chat_id in set(data.get("allowGroups", []))


def queue_line_group_pairing_request(chat_type: str, chat_id: str, user_id: str | None, text_preview: str) -> dict[str, Any]:
    data = load_line_group_pairing()
    now = utcnow()
    requests = data.get("requests", [])
    for item in requests:
        if item.get("chatId") == chat_id:
            item["chatType"] = chat_type
            item["lastSeenAt"] = now
            item["lastUserId"] = user_id
            item["lastTextPreview"] = text_preview[:500]
            item["requestCount"] = int(item.get("requestCount", 1)) + 1
            save_line_group_pairing(data)
            return item
    item = {
        "chatType": chat_type,
        "chatId": chat_id,
        "firstSeenAt": now,
        "lastSeenAt": now,
        "lastUserId": user_id,
        "lastTextPreview": text_preview[:500],
        "requestCount": 1,
    }
    requests.append(item)
    save_line_group_pairing(data)
    return item


def approve_line_group(chat_id: str) -> None:
    allow = load_line_allow_groups()
    if chat_id not in allow["allowGroups"]:
        allow["allowGroups"].append(chat_id)
        save_line_allow_groups(allow)
    pairing = load_line_group_pairing()
    pairing["requests"] = [item for item in pairing.get("requests", []) if item.get("chatId") != chat_id]
    save_line_group_pairing(pairing)


def reject_line_group(chat_id: str) -> None:
    pairing = load_line_group_pairing()
    pairing["requests"] = [item for item in pairing.get("requests", []) if item.get("chatId") != chat_id]
    save_line_group_pairing(pairing)


def load_line_group_settings() -> dict[str, Any]:
    ensure_line_auth_files()
    raw = read_json(LOCAL_LINE_GROUP_SETTINGS_PATH, {"version": 1, "groups": {}})
    raw.setdefault("version", 1)
    raw.setdefault("groups", {})
    return raw


def save_line_group_settings(data: dict[str, Any]) -> None:
    write_json(LOCAL_LINE_GROUP_SETTINGS_PATH, data)


def get_line_group_require_mention(chat_id: str, default_value: bool = True) -> bool:
    data = load_line_group_settings()
    entry = data.get("groups", {}).get(chat_id, {})
    if "requireMention" in entry:
        return bool(entry["requireMention"])
    return bool(default_value)


def set_line_group_require_mention(chat_id: str, enabled: bool) -> None:
    data = load_line_group_settings()
    data["groups"].setdefault(chat_id, {})
    data["groups"][chat_id]["requireMention"] = bool(enabled)
    data["groups"][chat_id]["updatedAt"] = utcnow()
    save_line_group_settings(data)


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS messages (
          id TEXT PRIMARY KEY,
          platform TEXT NOT NULL,
          chat_type TEXT NOT NULL,
          chat_id TEXT NOT NULL,
          user_id TEXT,
          line_message_id TEXT,
          line_reply_token TEXT,
          text_content TEXT,
          raw_event_json TEXT NOT NULL,
          dedup_key TEXT NOT NULL UNIQUE,
          session_key TEXT NOT NULL,
          binding_session_id TEXT,
          state TEXT NOT NULL,
          retry_count INTEGER NOT NULL DEFAULT 0,
          last_error TEXT,
          last_error_code TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          started_at TEXT,
          completed_at TEXT,
          delivered_at TEXT,
          manual_hold INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_messages_state ON messages(state);
        CREATE INDEX IF NOT EXISTS idx_messages_session_key ON messages(session_key);

        CREATE TABLE IF NOT EXISTS message_events (
          id TEXT PRIMARY KEY,
          message_id TEXT NOT NULL,
          event_type TEXT NOT NULL,
          old_state TEXT,
          new_state TEXT,
          actor TEXT NOT NULL,
          detail TEXT,
          created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_message_events_message_id ON message_events(message_id);

        CREATE TABLE IF NOT EXISTS bindings (
          session_key TEXT PRIMARY KEY,
          platform TEXT NOT NULL,
          chat_type TEXT NOT NULL,
          chat_id TEXT NOT NULL,
          codex_session_id TEXT NOT NULL,
          sdk_session_id TEXT,
          model TEXT,
          mode TEXT,
          workdir TEXT,
          active INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_bindings_chat ON bindings(platform, chat_id);

        CREATE TABLE IF NOT EXISTS runs (
          id TEXT PRIMARY KEY,
          message_id TEXT NOT NULL,
          binding_session_id TEXT,
          codex_session_id TEXT,
          attempt_no INTEGER NOT NULL,
          trigger_source TEXT NOT NULL,
          state TEXT NOT NULL,
          started_at TEXT,
          finished_at TEXT,
          exit_code INTEGER,
          error_text TEXT,
          created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_runs_message_id ON runs(message_id);
        CREATE INDEX IF NOT EXISTS idx_runs_state ON runs(state);

        CREATE TABLE IF NOT EXISTS deliveries (
          id TEXT PRIMARY KEY,
          message_id TEXT NOT NULL,
          run_id TEXT,
          channel TEXT NOT NULL,
          delivery_mode TEXT NOT NULL,
          target_id TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          state TEXT NOT NULL,
          provider_message_id TEXT,
          attempt_no INTEGER NOT NULL DEFAULT 1,
          error_text TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          sent_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_deliveries_message_id ON deliveries(message_id);
        CREATE INDEX IF NOT EXISTS idx_deliveries_state ON deliveries(state);

        CREATE TABLE IF NOT EXISTS artifacts (
          id TEXT PRIMARY KEY,
          message_id TEXT NOT NULL,
          run_id TEXT,
          kind TEXT NOT NULL,
          local_path TEXT NOT NULL,
          mime_type TEXT,
          file_size INTEGER,
          sha256 TEXT,
          created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS locks (
          lock_key TEXT PRIMARY KEY,
          owner TEXT NOT NULL,
          expires_at TEXT NOT NULL,
          created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS settings (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        """
    )
    conn.commit()


def import_openclaw_config() -> dict[str, Any]:
    cfg = default_config()
    write_json(CONFIG_PATH, cfg)
    return cfg


def log_event(conn: sqlite3.Connection, message_id: str, event_type: str, actor: str, detail: str = "", old_state: str | None = None, new_state: str | None = None) -> None:
    conn.execute(
        """
        INSERT INTO message_events (id, message_id, event_type, old_state, new_state, actor, detail, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (new_id(), message_id, event_type, old_state, new_state, actor, detail, utcnow()),
    )
    conn.commit()


def discord_request(config: dict[str, Any], path: str, payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    token = config.get("discord_bot_token") or ""
    req = request.Request(
        "https://discord.com/api/v10" + path,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bot {token}",
        },
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8") or "{}"
            return resp.status, json.loads(body)
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8") or "{}"
        try:
            data = json.loads(raw)
        except Exception:
            data = {"raw": raw}
        return exc.code, data


def alert(text: str, config: dict[str, Any] | None = None) -> None:
    append_log(ALERT_LOG_PATH, f"{utcnow()} {text}")
    cfg = config or load_config()
    channel_id = cfg.get("alarm_channel") or ""
    bot_token = cfg.get("discord_bot_token") or ""
    if not channel_id or not bot_token:
        return
    discord_request(
        cfg,
        f"/channels/{channel_id}/messages",
        {"content": f"[line-bridge alert]\n{text}"},
    )


def set_message_state(conn: sqlite3.Connection, message_id: str, new_state: str, actor: str, detail: str = "") -> None:
    if new_state not in MESSAGE_STATES:
        raise ValueError(f"invalid state: {new_state}")
    row = conn.execute("SELECT state FROM messages WHERE id = ?", (message_id,)).fetchone()
    if not row:
        return
    old_state = row["state"]
    conn.execute(
        "UPDATE messages SET state = ?, updated_at = ? WHERE id = ?",
        (new_state, utcnow(), message_id),
    )
    conn.commit()
    log_event(conn, message_id, "state_change", actor, detail, old_state, new_state)


def compute_signature(secret: str, body: bytes) -> str:
    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def parse_line_source(source: dict[str, Any]) -> tuple[str, str, str | None]:
    source_type = source.get("type") or "unknown"
    user_id = source.get("userId")
    if source_type == "group":
        return "group", source.get("groupId") or "", user_id
    if source_type == "room":
        return "room", source.get("roomId") or "", user_id
    return "direct", user_id or "", user_id


def session_key_for(chat_type: str, chat_id: str) -> str:
    return f"line:{chat_type}:{chat_id}"


def ensure_binding(conn: sqlite3.Connection, session_key: str, chat_type: str, chat_id: str, config: dict[str, Any]) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM bindings WHERE session_key = ?", (session_key,)).fetchone()
    if row:
        return row
    now = utcnow()
    codex_session_id = new_id()
    conn.execute(
        """
        INSERT INTO bindings (
          session_key, platform, chat_type, chat_id, codex_session_id, sdk_session_id,
          model, mode, workdir, active, created_at, updated_at
        ) VALUES (?, 'line', ?, ?, ?, NULL, ?, 'code', ?, 1, ?, ?)
        """,
        (session_key, chat_type, chat_id, codex_session_id, config.get("codex_model") or None, config.get("codex_workdir") or DEFAULT_WORKDIR, now, now),
    )
    conn.commit()
    row = conn.execute("SELECT * FROM bindings WHERE session_key = ?", (session_key,)).fetchone()
    return row


def rotate_binding_session(conn: sqlite3.Connection, session_key: str, reason: str, config: dict[str, Any]) -> str | None:
    row = conn.execute("SELECT * FROM bindings WHERE session_key = ?", (session_key,)).fetchone()
    if not row:
        return None
    new_session_id = new_id()
    conn.execute(
        """
        UPDATE bindings
        SET codex_session_id = ?, model = ?, workdir = ?, updated_at = ?
        WHERE session_key = ?
        """,
        (
            new_session_id,
            config.get("codex_model") or None,
            config.get("codex_workdir") or DEFAULT_WORKDIR,
            utcnow(),
            session_key,
        ),
    )
    conn.commit()
    alert(f"rotated session for {session_key}: {reason}")
    return new_session_id


def conversation_prompt(conn: sqlite3.Connection, session_key: str, current_message_id: str) -> str:
    rows = conn.execute(
        """
        SELECT text_content, chat_type, chat_id, created_at, id
        FROM messages
        WHERE session_key = ? AND text_content IS NOT NULL
        ORDER BY created_at DESC
        LIMIT 8
        """,
        (session_key,),
    ).fetchall()
    history = list(reversed(rows))
    parts = [
        "You are replying inside a LINE chat bridge.",
        "Reply briefly and directly unless the user asked for detail.",
        "Do not mention internal bridge state.",
        "If the user wants an attached image, file, PDF, or screenshot sent back, include the absolute local artifact path on its own line in the reply.",
        "",
        "Recent conversation:",
    ]
    for row in history:
        prefix = "Current user message" if row["id"] == current_message_id else "Previous user message"
        parts.append(f"- {prefix}: {row['text_content']}")
        artifacts = conn.execute(
            "SELECT kind, local_path FROM artifacts WHERE message_id = ? ORDER BY created_at ASC",
            (row["id"],),
        ).fetchall()
        for artifact in artifacts:
            parts.append(f"  Attached {artifact['kind']}: {artifact['local_path']}")
    parts.append("")
    parts.append("Write the reply text only.")
    return "\n".join(parts)


def run_codex(prompt: str, config: dict[str, Any]) -> tuple[int, str, str]:
    workdir = config.get("codex_workdir") or DEFAULT_WORKDIR
    model = config.get("codex_model") or ""
    with tempfile.NamedTemporaryFile(prefix="line-bridge-reply-", suffix=".txt", delete=False) as f:
        output_path = f.name
    cmd = [
        CODEX_BIN,
        "exec",
        "--skip-git-repo-check",
        "--dangerously-bypass-approvals-and-sandbox",
        "-C",
        workdir,
        "-o",
        output_path,
        prompt,
    ]
    if model:
        cmd[2:2] = ["-m", model]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    try:
        reply = pathlib.Path(output_path).read_text().strip()
    except Exception:
        reply = ""
    finally:
        pathlib.Path(output_path).unlink(missing_ok=True)
    stderr = (proc.stderr or "").strip()
    return proc.returncode, reply, stderr


def line_request(config: dict[str, Any], path: str, payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    token = config.get("line_channel_access_token") or ""
    req = request.Request(
        "https://api.line.me" + path,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8") or "{}"
            return resp.status, json.loads(body)
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8") or "{}"
        try:
            data = json.loads(raw)
        except Exception:
            data = {"raw": raw}
        return exc.code, data


def line_binary_request(config: dict[str, Any], path: str) -> tuple[int, bytes, dict[str, str]]:
    token = config.get("line_channel_access_token") or ""
    req = request.Request(
        "https://api-data.line.me" + path,
        headers={
            "Authorization": f"Bearer {token}",
        },
        method="GET",
    )
    try:
        with request.urlopen(req, timeout=60) as resp:
            return resp.status, resp.read(), dict(resp.headers.items())
    except error.HTTPError as exc:
        return exc.code, exc.read(), dict(exc.headers.items())


def sanitize_filename(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._")
    return cleaned or "file"


def detect_kind_from_path(path: str) -> str:
    mime, _ = mimetypes.guess_type(path)
    lower = path.lower()
    if mime and mime.startswith("image/"):
        return "image"
    if mime == "application/pdf" or lower.endswith(".pdf"):
        return "pdf"
    return "file"


def register_artifact(
    conn: sqlite3.Connection,
    message_id: str,
    run_id: str | None,
    local_path: str,
    kind: str | None = None,
    mime_type: str | None = None,
) -> str:
    path = pathlib.Path(local_path).expanduser().resolve()
    artifact_id = new_id()
    final_name = sanitize_filename(path.name)
    stored_path = ARTIFACTS_DIR / f"{artifact_id}_{final_name}"
    if path != stored_path:
        stored_path.write_bytes(path.read_bytes())
    actual_kind = kind or detect_kind_from_path(str(stored_path))
    actual_mime = mime_type or mimetypes.guess_type(str(stored_path))[0] or "application/octet-stream"
    conn.execute(
        """
        INSERT INTO artifacts (
          id, message_id, run_id, kind, local_path, mime_type, file_size, sha256, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            artifact_id,
            message_id,
            run_id,
            actual_kind,
            str(stored_path),
            actual_mime,
            stored_path.stat().st_size,
            sha256_file(str(stored_path)),
            utcnow(),
        ),
    )
    conn.commit()
    return artifact_id


def artifact_public_url(config: dict[str, Any], artifact_id: str, filename: str) -> str:
    base = (config.get("public_base_url") or "").rstrip("/")
    if not base:
        return ""
    prefix = "/" + (config.get("media_path_prefix") or "/line/media").strip("/")
    return f"{base}{prefix}/{artifact_id}/{sanitize_filename(filename)}"


def extract_local_paths(text: str) -> list[str]:
    paths: list[str] = []
    for match in re.finditer(r"(/[^\\s<>()\\[\\]\"']+)", text):
        candidate = match.group(1).rstrip(".,)")
        if candidate in paths:
            continue
        try:
            if pathlib.Path(candidate).expanduser().exists():
                paths.append(candidate)
        except Exception:
            continue
    return paths


def strip_local_paths(text: str) -> str:
    cleaned_lines: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("/") and pathlib.Path(stripped).expanduser().exists():
            continue
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines).strip()


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()
