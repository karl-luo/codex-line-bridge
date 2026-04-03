---
name: line-bridge
description: Standalone LINE bridge for Codex with its own webhook, queue, session bindings, sender, and watcher. Can optionally import LINE credentials from an existing OpenClaw install.
metadata:
  short-description: Dedicated LINE bridge for Codex
---

# line-bridge

Use this skill when the user wants a dedicated LINE bridge that is separate from `claude-to-im` and separate from OpenClaw.

This skill is designed around five roles:

- `webhook_server.py` receives and verifies LINE webhook events and stores them in sqlite.
- `runner.py` dequeues pending user messages and runs Codex for them.
- `sender.py` delivers completed replies back to LINE using reply or push.
- `watcher.py` detects stalled messages and retries the right layer.
- `bootstrap.py` initializes config and database state.

## Config source

This skill can optionally import LINE credentials from:

- `$OPENCLAW_HOME/openclaw.json`

It writes its own private runtime config to:

- `data/config.json`

## First-time setup

```bash
python3 scripts/bootstrap.py --import-openclaw
```

## Runtime

Start the three long-running workers in separate terminals or tmux panes:

```bash
python3 scripts/webhook_server.py
python3 scripts/runner.py
python3 scripts/sender.py
python3 scripts/watcher.py
```

Or use the management CLI:

```bash
python3 scripts/line_bridge_ctl.py status
python3 scripts/line_bridge_ctl.py start
python3 scripts/line_bridge_ctl.py stop
python3 scripts/line_bridge_ctl.py inspect <message-id>
python3 scripts/line_bridge_ctl.py retry <message-id>
```

## Operational notes

- The bridge stores all message state in sqlite and only mirrors small config to JSON.
- LINE delivery is split from Codex execution, so a failed send does not rerun the model.
- `replyToken` is used first. If it expires, sender falls back to push when possible.
- The first version keeps a logical per-chat session in sqlite. It does not yet try to resume native Codex CLI sessions.

## Files

- Config: `data/config.json`
- Database: `data/line_bridge.sqlite3`
- Logs: `logs/`

## Safe defaults

- Private chats bind to `line:direct:<userId>`
- Groups bind to `line:group:<groupId>`
- Rooms bind to `line:room:<roomId>`
- Runner uses the current working directory unless `CODEX_WORKDIR` is set
- The default model is left unset so Codex uses its own default unless configured in `config.json`
