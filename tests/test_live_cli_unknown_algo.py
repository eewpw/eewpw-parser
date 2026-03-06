import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ENV = os.environ.copy()
ENV["PYTHONPATH"] = str(ROOT / "src")
CLI = [sys.executable, str(ROOT / "src" / "eewpw_parser" / "live_cli.py")]


def test_live_cli_rejects_unknown_algo():
    with tempfile.TemporaryDirectory() as td:
        dummy_log = Path(td) / "dummy.log"
        dummy_log.write_text("line", encoding="utf-8")
        cmd = CLI + ["--algo", "foo", "--dialect", "scfinder", "--logfile", str(dummy_log)]
        result = subprocess.run(cmd, capture_output=True, text=True, env=ENV, cwd=td)
        assert result.returncode != 0
        assert "invalid choice" in (result.stderr or result.stdout)
