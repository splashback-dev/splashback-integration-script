from argparse import Namespace
from mimetypes import guess_extension
from pathlib import Path
from typing import List

import requests

from finder import BaseFinder


class RequestFinder(BaseFinder):
    def __init__(self, app_dir: Path):
        super().__init__(app_dir)

    def start_interactive(self) -> List[Path]:
        raise NotImplementedError()

    def start_silent(self, args: Namespace) -> List[Path]:
        # TODO: Remove! Use FTP for BOM data...
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:90.0) Gecko/20100101 Firefox/90.0'}

        with requests.get(args.request_url, stream=True, headers=headers) as r:
            r.raise_for_status()

            file_name = args.request_url.split('/')[-1]
            if '.' not in file_name:
                file_ext = guess_extension(r.headers['content-type'])
                file_name = file_name + '.' + file_ext
            path = self._app_dir.joinpath(file_name)

            if path.exists():
                # TODO: handle last modified date to update old datasets.
                #  we will also need to delete data from Splashback in the old dataset...
                return [path]

            path.parent.mkdir(parents=True, exist_ok=True)

            with path.open('wb') as file:
                for chunk in r.iter_content(chunk_size=8192):
                    file.write(chunk)
        return [path]
