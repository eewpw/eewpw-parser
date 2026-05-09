# -*- coding: utf-8 -*-
import logging
import os
import time
from typing import Iterable, List, Tuple

logger = logging.getLogger(__name__)


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
    def __init__(
        self,
        path: str,
        poll_interval: float = 0.1,
        seek_end: bool = True,
        max_lines: int | None = None,
        follow: bool = True,
    ):
        self.path = path
        self.poll_interval = poll_interval
        self.seek_end = seek_end
        self.max_lines = max_lines
        self.follow = follow

    @staticmethod
    def _file_sig_from_stat(st: os.stat_result) -> Tuple[int, int]:
        return int(st.st_dev), int(st.st_ino)

    def _open_for_tail(self):
        return open(self.path, "r", encoding="utf-8", errors="ignore")

    def __iter__(self) -> Iterable[str]:
        f = None
        file_sig = None
        missing_logged = False
        lines_yielded = 0
        partial_line = ""
        seek_end_on_open = bool(self.seek_end)

        try:
            while f is None:
                try:
                    f = self._open_for_tail()
                except FileNotFoundError:
                    if not self.follow:
                        raise
                    if not missing_logged:
                        logger.info("tail source path is missing, waiting: %s", self.path)
                        missing_logged = True
                    time.sleep(self.poll_interval)
                    continue
                st = os.fstat(f.fileno())
                file_sig = self._file_sig_from_stat(st)
                if seek_end_on_open:
                    f.seek(0, os.SEEK_END)
                    seek_end_on_open = False
                if missing_logged:
                    logger.info("tail source path is available again: %s", self.path)
                    missing_logged = False

            while True:
                line = f.readline()
                if line:
                    if self.follow:
                        if partial_line:
                            line = partial_line + line
                            partial_line = ""
                        if not line.endswith("\n"):
                            partial_line = line
                            continue
                    yield line
                    lines_yielded += 1

                    if self.max_lines is not None and lines_yielded >= self.max_lines:
                        break
                    continue

                if not self.follow:
                    if partial_line:
                        yield partial_line
                        lines_yielded += 1
                    break

                try:
                    pst = os.stat(self.path)
                    if missing_logged:
                        logger.info("tail source path is available again: %s", self.path)
                        missing_logged = False
                except FileNotFoundError:
                    if not missing_logged:
                        logger.info("tail source path is missing, waiting: %s", self.path)
                        missing_logged = True
                    time.sleep(self.poll_interval)
                    continue

                path_sig = self._file_sig_from_stat(pst)
                if path_sig != file_sig:
                    new_f = None
                    try:
                        new_f = self._open_for_tail()
                        new_st = os.fstat(new_f.fileno())
                        new_sig = self._file_sig_from_stat(new_st)
                    except FileNotFoundError:
                        if new_f is not None:
                            new_f.close()
                        if not missing_logged:
                            logger.info("tail source path is missing, waiting: %s", self.path)
                            missing_logged = True
                        time.sleep(self.poll_interval)
                        continue

                    if new_sig == path_sig:
                        partial_line = ""
                        f.close()
                        f = new_f
                        file_sig = new_sig
                        logger.info("tail source reopened after rotation: %s", self.path)
                        continue

                    new_f.close()
                    time.sleep(self.poll_interval)
                    continue

                cur_pos = f.tell()
                if pst.st_size < cur_pos:
                    partial_line = ""
                    f.seek(0, os.SEEK_SET)
                    logger.info("tail source detected truncate-in-place, rewound: %s", self.path)
                    continue

                time.sleep(self.poll_interval)
        finally:
            if f is not None:
                f.close()
