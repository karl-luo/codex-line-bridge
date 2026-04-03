# Codex LINE Bridge

Version: `1.0.2`

Standalone LINE bridge for Codex with its own webhook, queue, session bindings, sender, watcher, pairing allowlist, media handling, and Discord alerts.

## Features

- LINE webhook verification with `channelSecret`
- Dedicated async workers:
  - `webhook_server.py`
  - `runner.py`
  - `sender.py`
  - `watcher.py`
- SQLite-backed message, run, delivery, and artifact state
- Shared-style LINE pairing flow:
  - pending requests
  - allowlist approvals
  - auto-reject for unauthorized users
- Group approval and mention controls:
  - pending group requests
  - group allowlist approvals
  - per-group `requireMention` override
  - default group mode requires bot mention
- Rich media support:
  - inbound images
  - inbound files
  - image resend
  - image merge to PDF
  - screenshot/PDF/file outbound links
- Discord alerting for failures and pairing requests

## Repository layout

- `scripts/`: runtime scripts
- `examples/config.example.json`: sanitized config template
- `examples/line-allowFrom.example.json`: empty allowlist template
- `examples/line-pairing.example.json`: empty pairing queue template
- `examples/line-allowGroups.example.json`: empty group allowlist template
- `examples/line-group-pairing.example.json`: empty group approval queue template
- `examples/line-group-settings.example.json`: empty per-group settings template

## Quick start

1. Copy `examples/config.example.json` to `data/config.json`
2. Fill in:
   - `line_channel_access_token`
   - `line_channel_secret`
   - `discord_bot_token`
   - `public_base_url`
3. Create local auth state files from the examples:
   - `data/line-allowFrom.json`
   - `data/line-pairing.json`
   - `data/line-allowGroups.json`
   - `data/line-group-pairing.json`
   - `data/line-group-settings.json`
4. Install requirements:

```bash
python3 -m pip install -r requirements.txt
```

5. Start the workers:

```bash
python3 scripts/webhook_server.py
python3 scripts/runner.py
python3 scripts/sender.py
python3 scripts/watcher.py
```

## Notes

- This repository intentionally excludes real secrets, API keys, runtime databases, logs, and media artifacts.
- LINE cannot natively send arbitrary PDF attachments the same way Discord can. The current sender delivers PDFs as public download links.
- New direct chats still require approval when `line_dm_policy` is `pairing`.
- New groups and rooms can also require approval, and approved groups default to mention-only handling unless overridden.

## License

MIT
