from argparse import Namespace
from typing.io import IO


class BaseFinder:
    def __init__(self):
        pass

    def start_interactive(self) -> IO:
        raise NotImplementedError()

    def start_silent(self, args: Namespace) -> IO:
        raise NotImplementedError()
