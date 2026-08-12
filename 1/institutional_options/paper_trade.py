"""Entry point for the live paper trading program with visual dashboard.

    python -m institutional_options.paper_trade

Starts the Fyers-backed paper runner (no orders placed) and serves a live
dashboard at http://127.0.0.1:8765 (configurable via uploads/PAPER_RUNNER.json).

First run requires one interactive Fyers login (the auth URL is printed);
the access token is then saved to paper_state/tokens.json and auto-refreshes.
"""
from __future__ import annotations

import json
import threading
from pathlib import Path

from .config import SystemConfig
from .paper_dashboard import PaperDashboard
from .paper_runner import PaperRunner


def main() -> int:
    cfg = SystemConfig.from_file("uploads/PARAMETERS.json")
    runner_cfg_path = Path("uploads/PAPER_RUNNER.json")
    runner_cfg = json.loads(runner_cfg_path.read_text(encoding="utf-8")) if runner_cfg_path.exists() else {}

    runner = PaperRunner(cfg, runner_cfg)
    dash_cfg = runner_cfg.get("dashboard", {})
    dashboard = PaperDashboard(
        runner.snapshot,
        host=str(dash_cfg.get("host", "127.0.0.1")),
        port=int(dash_cfg.get("port", 8765)),
    )
    url = dashboard.start()
    print(f"\nDashboard: {url}  (Ctrl+C to stop)")
    stop = threading.Event()
    try:
        runner.run_forever(stop_event=stop)
    except KeyboardInterrupt:
        print("\nStopping paper runner…")
    finally:
        dashboard.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
