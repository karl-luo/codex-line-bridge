#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from common import CONFIG_PATH, DB_PATH, connect_db, import_openclaw_config, init_db, load_config


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--import-openclaw", action="store_true")
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()

    conn = connect_db()
    init_db(conn)

    if args.import_openclaw:
        cfg = import_openclaw_config()
        print(f"Imported LINE config into {CONFIG_PATH}")
        print(json.dumps({
            "has_token": bool(cfg.get("line_channel_access_token")),
            "has_secret": bool(cfg.get("line_channel_secret")),
            "webhook_port": cfg.get("webhook_port"),
            "webhook_path": cfg.get("webhook_path"),
            "workdir": cfg.get("codex_workdir"),
        }, indent=2))
        return 0

    if args.status:
        cfg = load_config()
        print(json.dumps({
            "config_path": str(CONFIG_PATH),
            "db_path": str(DB_PATH),
            "has_token": bool(cfg.get("line_channel_access_token")),
            "has_secret": bool(cfg.get("line_channel_secret")),
            "webhook_port": cfg.get("webhook_port"),
            "webhook_path": cfg.get("webhook_path"),
            "codex_workdir": cfg.get("codex_workdir"),
        }, indent=2))
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
