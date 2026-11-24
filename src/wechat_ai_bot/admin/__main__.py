"""Run the dashboard on the local machine."""

import argparse
import os
from pathlib import Path
import sys
import webbrowser

from wechat_ai_bot.admin import AdminServer, AdminState, Dashboard, RUNTIME_CONFIG


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path.cwd() / "config.json",
                        help="Local reference configuration and read-only Daemon credential path")
    parser.add_argument("--runtime-config", type=Path, default=RUNTIME_CONFIG,
                        help="Deployed configuration, when readable by the server")
    data_home = Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local/state")
    parser.add_argument("--state-dir", type=Path, default=data_home / "wechat-ai-bot/admin")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--open", action="store_true", help="Open the local dashboard in the desktop browser")
    parser.add_argument("--show-key", action="store_true", help="Show the dashboard key in your own interactive terminal")
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    if args.show_key and not sys.stdout.isatty():
        parser.error("Read the access key in an interactive terminal")
    try:
        state = AdminState(args.state_dir)
        if args.show_key:
            print(state.key)
            return 0
        with AdminServer(args.port, state, Dashboard(args.config, args.runtime_config)) as server:
            url = f"http://127.0.0.1:{server.server_port}/"
            print(f"Local dashboard {url}", flush=True)
            print(f"Access key file {state.key_path}", flush=True)
            if args.open:
                webbrowser.open(url)
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
    except (OSError, ValueError) as exc:
        print(f"Dashboard startup failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
