# -*- coding: utf-8 -*-
import os
import tempfile
from pathlib import Path


def pytest_configure():
    root = Path(__file__).resolve().parent.parent
    tmp_dir = root / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = str(tmp_dir)
    os.environ["TMPDIR"] = tmp_path
    os.environ["TEMP"] = tmp_path
    os.environ["TMP"] = tmp_path
    tempfile.tempdir = tmp_path
