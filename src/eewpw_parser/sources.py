# -*- coding: utf-8 -*-
import os
import time
from typing import Iterable, List, Tuple


class LineSource:
    """
    Abstract line source; subclasses must implement __iter__ to yield raw log lines (str),
    including trailing newline if present.
    """

    def __iter__(self) -> Iterable[str]:
        raise NotImplementedError()


class ReplayLineSource(LineSource):
    def __init__(self, files: List[str], encoding: str = "utf-8", errors: str = "ignore"):
        self._files = files
        self._encoding = encoding
        self._errors = errors

    def iterate_files(self) -> Iterable[Tuple[str, Iterable[str]]]:
        for path in self._files:
            f = open(path, "r", encoding=self._encoding, errors=self._errors)
            try:
                yield path, f
            finally:
                f.close()

    def __iter__(self) -> Iterable[str]:
        for _, lines in self.iterate_files():
            for line in lines:
                yield line


class TailLineSource(LineSource):
    def __init__(self, path: str, poll_interval: float = 0.1, seek_end: bool = True, max_lines: int | None = None):
        self.path = path
        self.poll_interval = poll_interval
        self.seek_end = seek_end
        self.max_lines = max_lines

    def __iter__(self) -> Iterable[str]:
        with open(self.path, "r", encoding="utf-8", errors="ignore") as f:
            if self.seek_end:
                f.seek(0, os.SEEK_END)

            lines_yielded = 0
            while True:
                line = f.readline()
                if not line:
                    time.sleep(self.poll_interval)
                    continue

                yield line
                lines_yielded += 1

                if self.max_lines is not None and lines_yielded >= self.max_lines:
                    break
