"""The command boundary must be usable without credentials or a database."""

import os
import subprocess
import sys


def test_help_is_available_without_credentials():
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in {"DATABASE_URL", "TIKTOK_DISPLAY_ACCESS_TOKEN", "TIKTOK_CCL_ACCESS_TOKEN"}
    }
    result = subprocess.run(
        [sys.executable, "-m", "sm_trending_products", "--help"],
        capture_output=True,
        text=True,
        env=environment,
    )
    assert result.returncode == 0
    assert "backtest" in result.stdout
    assert "verify" in result.stdout
