import os
import shutil
import sys
from argparse import ArgumentParser
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Type

from dotenv import load_dotenv
from splashback_data.model.import_results import ImportResults
from splashback_data.model.model_import import ModelImport

from finder import BaseFinder
from finder.request import RequestFinder
from finder.thredds import ThreddsFinder
from parser import BaseParser
from parser.json import JsonParser
from parser.netcdf import NetcdfParser
from splashback import SplashbackImporter


def select_finder(app_dir: Path) -> BaseFinder:
    print('Select a finder')
    finders: List[Type[BaseFinder]] = [ThreddsFinder]
    for idx, finder_type in enumerate(finders):
        print(f'[{idx}] {finder_type.__name__}')
    selected_str = input('>> ').strip()

    try:
        selected_idx = int(selected_str)
    except ValueError:
        return select_finder(app_dir)

    if 0 <= selected_idx < len(finders):
        return finders[selected_idx](app_dir)
    return select_finder(app_dir)


def select_parser(path: Path) -> BaseParser:
    print('Select a parser')
    parsers: List[Type[BaseParser]] = [NetcdfParser]
    for idx, parser_type in enumerate(parsers):
        print(f'[{idx}] {parser_type.__name__}')
    selected_str = input('>> ').strip()

    try:
        selected_idx = int(selected_str)
    except ValueError:
        return select_parser(path)

    if 0 <= selected_idx < len(parsers):
        return parsers[selected_idx](path)
    return select_parser(path)


def main_interactive(app_dir: Path) -> None:
    m_finder = select_finder(app_dir)
    paths = m_finder.start_interactive()
    assert len(paths) == 1
    path = paths[0]

    m_parser = select_parser(path)
    imports = m_parser.start_interactive()

    m_importer = SplashbackImporter(os.environ['SPLASHBACK_API_KEY'], args.pool_id)
    check_results = m_importer.check(imports)
    metadata = m_parser.start_metadata_interactive(check_results)

    line_len = shutil.get_terminal_size()[0]
    for name, current, count in m_importer.create_metadata(metadata):
        end = '\n' if current == count else '\r'
        print(f'{name} ({current}/{count})'.ljust(line_len), end=end)
    m_importer.run(imports)


def main_silent(app_dir: Path) -> None:
    if args.finder == 'thredds':
        m_finder = ThreddsFinder(app_dir)
    elif args.finder == 'request':
        m_finder = RequestFinder(app_dir)
    else:
        raise Exception(f'Unknown finder: {args.finder}')

    paths = m_finder.start_silent(args)

    # Start from start path if provided
    if args.start_path is not None:
        start_path = Path(args.start_path)
        start_path_idxs = [idx for idx, path in enumerate(paths) if path.relative_to(app_dir) == start_path]
        if len(start_path_idxs) != 1:
            raise Exception(f'Failed to find first path: {start_path}')
        paths = [path for idx, path in enumerate(paths) if idx >= start_path_idxs[0]]

    # Create importer
    m_importer = SplashbackImporter(os.environ['SPLASHBACK_API_KEY'], args.pool_id)

    # Process all paths
    while len(paths) > 0:
        # Generate batch
        batch_imports: List[ModelImport] = []
        batch_parsers: List[BaseParser] = []
        batch_sizes: List[int] = []
        batch_paths: List[Path] = []
        for path in paths:
            if args.parser == 'netcdf':
                m_parser = NetcdfParser(path)
            elif args.parser == 'json':
                m_parser = JsonParser(path)
            else:
                raise Exception(f'Unknown parser: {args.parser}')

            path_imports = m_parser.start_silent(args)

            # If we have at least one import in the batch and have reached the batch size, break
            if len(batch_imports) != 0 and len(batch_imports) + len(path_imports) > args.batch_size:
                break

            # Append path imports
            batch_imports += path_imports
            batch_parsers += [m_parser]
            batch_sizes += [len(path_imports)]
            batch_paths += [path]

        # Remove batched paths
        paths = paths[len(batch_paths):]

        if args.verbose:
            print(f'Completed batch: {batch_imports}')

        # Check batch
        check_results = m_importer.check(batch_imports)

        # Generate missing metadata
        metadata = None
        for idx, m_parser in enumerate(batch_parsers):
            m_start_idx = sum(batch_sizes[0:idx])
            m_end_idx = m_start_idx + batch_sizes[idx]
            m_messages = [m for m in check_results['messages'] if m_start_idx <= m['index'] < m_end_idx]
            for m in m_messages:
                m['index'] -= m_start_idx
            m_check_results = ImportResults._from_openapi_data(messages=m_messages)
            metadata = m_parser.start_metadata_silent(m_check_results, args, metadata=metadata)

        if metadata is not None:
            # Create metadata
            line_len = shutil.get_terminal_size()[0]
            for name, current, count in m_importer.create_metadata(metadata):
                if args.verbose:
                    end = '\n' if current == count else '\r'
                    print(f'{name} ({current}/{count})'.ljust(line_len), end=end)

        # Import batch
        option_skip_exist_sample = 'skip_exist_sample' in args.option if type(args.option) is list else False
        result = m_importer.run(batch_imports, dry_run=args.dry_run, skip_exist_sample=option_skip_exist_sample)

        print(f'Imported {result["imported_sample_count"]} samples,'
              f' {result["imported_variant_count"]} variants and'
              f' {result["imported_value_count"]} values from'
              f' {",".join([str(path.relative_to(app_dir)) for path in batch_paths])}')


if __name__ == '__main__':
    # Setup argument parser
    parser = ArgumentParser()
    parser.add_argument('-i', '--interactive', action='store_true',
                        help='Enable interactive mode, ignoring other CLI arguments.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Enable verbose mode.')

    parser.add_argument('--pool-id', type=str,
                        help='Splashback Pool ID to integrate.')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Number of imports per batch.')
    parser.add_argument('--dry-run', action='store_true',
                        help='Do not publish the data to Splashback.')
    parser.add_argument('-d', '--dir', type=str,
                        help='Directory to store data. If unspecified a temporary directory will be used.')
    parser.add_argument('--start-path', type=str,
                        help='Path to start importing from. Useful if an import was interrupted and should be resumed.')
    # Add valid finder choices here
    parser.add_argument('-f', '--finder', type=str, choices=['thredds', 'request'],
                        help='Finder to locate and fetch data.')
    # Add valid parser choices here
    parser.add_argument('-p', '--parser', type=str, choices=['netcdf', 'json'],
                        help='Parser to read fetched data.')
    parser.add_argument('-o', '--option', type=str, action='append',
                        choices=['ignore_zero_dups', 'ignore_dups', 'skip_exist_sample'],
                        help='Additional options.')

    args_no_help = [a for a in sys.argv if a != '-h' and a != '--help']
    current_args = parser.parse_known_args(args_no_help)[0]

    # Add finders
    is_finder_thredds = not current_args.interactive and current_args.finder == 'thredds'
    parser_group = parser.add_argument_group(title='Finder: thredds',
                                             description='Work with a THREDDS data source.')
    parser_group.add_argument('--thredds-dataset', type=str, required=is_finder_thredds,
                              help='THREDDS Dataset ID to download.')
    parser_group.add_argument('--thredds-dataset-pattern', action='store_true',
                              help='Enable pattern matching for THREDDS Dataset ID.')
    parser_group.add_argument('--thredds-service', type=str, required=is_finder_thredds,
                              help='THREDDS Service name to use for dataset download.')
    is_finder_request = not current_args.interactive and current_args.finder == 'request'
    parser_group = parser.add_argument_group(title='Finder: request',
                                             description='Make a web API request.')
    parser_group.add_argument('--request-url', type=str, required=is_finder_request,
                              help='URL to make the web API request to.')

    # Add parsers
    is_parser_netcdf = not current_args.interactive and current_args.parser == 'netcdf'
    parser_group = parser.add_argument_group(title='Parser: netcdf',
                                             description='Read NetCDF files.')
    parser_group.add_argument('--netcdf-mapping', type=str, required=is_parser_netcdf,
                              help='Field mapping file for the NetCDF parser.')
    is_parser_json = not current_args.interactive and current_args.parser == 'json'
    parser_group = parser.add_argument_group(title='Parser: json',
                                             description='Read JSON files.')
    parser_group.add_argument('--json-mapping', type=str, required=is_parser_json,
                              help='Field mapping file for the JSON parser.')

    args = parser.parse_args()

    # Load .env file
    load_dotenv()

    # Use passed directory
    if args.dir is not None:
        args_dir = Path(args.dir)
        if not args_dir.is_dir():
            raise Exception(f'The given path is not a directory: {args_dir}')

        if args.interactive:
            main_interactive(args_dir)
        else:
            main_silent(args_dir)

    # Use temporary directory
    else:
        with TemporaryDirectory() as tmp_dir:
            if args.interactive:
                main_interactive(Path(tmp_dir))
            else:
                main_silent(Path(tmp_dir))
