from argparse import Namespace
from pathlib import Path
from typing import List


class BaseFinder:
    def __init__(self, app_dir: Path):
        self._app_dir: Path = app_dir

    def start_interactive(self) -> List[Path]:
        raise NotImplementedError()

    def start_silent(self, args: Namespace) -> List[Path]:
        raise NotImplementedError()
