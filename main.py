import os
import shutil
import sys
from argparse import ArgumentParser
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Type

from finder import BaseFinder
from finder.thredds import ThreddsFinder
from parser import BaseParser
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

    m_importer = SplashbackImporter(os.environ['SPLASHBACK_API_KEY'], args.pool_id, imports)
    check_results = m_importer.check()
    metadata = m_parser.start_metadata_interactive(check_results)

    line_len = shutil.get_terminal_size()[0]
    for name, current, count in m_importer.create_metadata(metadata):
        end = '\n' if current == count else '\r'
        print(f'{name} ({current}/{count})'.ljust(line_len), end=end)
    m_importer.run()


def main_silent(app_dir: Path) -> None:
    if args.finder == 'thredds':
        m_finder = ThreddsFinder(app_dir)
    else:
        raise Exception(f'Unknown finder: {args.finder}')

    paths = m_finder.start_silent(args)

    for path in paths:
        if args.parser == 'netcdf':
            m_parser = NetcdfParser(path)
        else:
            raise Exception(f'Unknown parser: {args.parser}')

        imports = m_parser.start_silent(args)

        option_ignore_zero_dups = 'ignore_zero_dups' in args.option if type(args.option) is list else False
        option_ignore_dups = 'ignore_dups' in args.option if type(args.option) is list else False
        option_skip_exist_sample = 'skip_exist_sample' in args.option if type(args.option) is list else False

        m_importer = SplashbackImporter(os.environ['SPLASHBACK_API_KEY'], args.pool_id, imports,
                                        ignore_zero_dups=option_ignore_zero_dups,
                                        ignore_dups=option_ignore_dups)
        check_results = m_importer.check()
        metadata = m_parser.start_metadata_silent(check_results, args)

        line_len = shutil.get_terminal_size()[0]
        for name, current, count in m_importer.create_metadata(metadata):
            if args.verbose:
                end = '\n' if current == count else '\r'
                print(f'{name} ({current}/{count})'.ljust(line_len), end=end)
        result = m_importer.run(args.dry_run, option_skip_exist_sample)

        print(f'Imported {result["imported_sample_count"]} samples,'
              f' {result["imported_variant_count"]} variants and'
              f' {result["imported_value_count"]} values from {path}')


if __name__ == '__main__':
    # Setup argument parser
    parser = ArgumentParser()
    parser.add_argument('-i', '--interactive', action='store_true',
                        help='Enable interactive mode, ignoring other CLI arguments.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Enable verbose mode.')

    parser.add_argument('--pool-id', type=str,
                        help='Splashback Pool ID to integrate.')
    parser.add_argument('--dry-run', action='store_true',
                        help='Do not publish the data to Splashback.')
    parser.add_argument('-d', '--dir', type=str,
                        help='Directory to store data. If unspecified a temporary directory will be used.')
    parser.add_argument('-f', '--finder', type=str, choices=['thredds'],
                        help='Finder to locate and fetch data.')
    parser.add_argument('-p', '--parser', type=str, choices=['netcdf'],
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

    # Add parsers
    is_parser_netcdf = not current_args.interactive and current_args.parser == 'netcdf'
    parser_group = parser.add_argument_group(title='Parser: netcdf',
                                             description='Read NetCDF files.')
    parser_group.add_argument('--netcdf-mapping', type=str, required=is_parser_netcdf,
                              help='Field mapping file for the NetCDF parser.')

    args = parser.parse_args()

    # Use passed directory
    if args.dir is not None:
        app_dir = Path(args.dir)
        if not app_dir.is_dir():
            raise Exception(f'The given path is not a directory: {app_dir}')

        if args.interactive:
            main_interactive(app_dir)
        else:
            main_silent(app_dir)

    # Use temporary directory
    else:
        with TemporaryDirectory() as tmp_dir:
            if args.interactive:
                main_interactive(Path(tmp_dir))
            else:
                main_silent(Path(tmp_dir))
