import os
import shutil
import sys
from argparse import ArgumentParser
from typing import List, Type
from typing.io import IO

from finder import BaseFinder
from finder.thredds import ThreddsFinder
from parser import BaseParser
from parser.netcdf import NetcdfParser
from splashback import SplashbackImporter


def select_finder() -> BaseFinder:
    print('Select a finder')
    finders: List[Type[BaseFinder]] = [ThreddsFinder]
    for idx, finder_type in enumerate(finders):
        print(f'[{idx}] {finder_type.__name__}')
    selected_str = input('>> ').strip()

    try:
        selected_idx = int(selected_str)
    except ValueError:
        return select_finder()

    if 0 <= selected_idx < len(finders):
        return finders[selected_idx]()
    return select_finder()


def select_parser(file: IO) -> BaseParser:
    print('Select a parser')
    parsers: List[Type[BaseParser]] = [NetcdfParser]
    for idx, parser_type in enumerate(parsers):
        print(f'[{idx}] {parser_type.__name__}')
    selected_str = input('>> ').strip()

    try:
        selected_idx = int(selected_str)
    except ValueError:
        return select_parser(file)

    if 0 <= selected_idx < len(parsers):
        return parsers[selected_idx](file)
    return select_parser(file)


def main_interactive() -> None:
    m_finder = select_finder()
    file = m_finder.start_interactive()

    try:
        m_parser = select_parser(file)
        imports = m_parser.start_interactive()

        m_importer = SplashbackImporter(os.environ['SPLASHBACK_API_KEY'], args.pool_id, imports)
        check_results = m_importer.check()
        metadata = m_parser.start_metadata_interactive(check_results)

    finally:
        file.close()

    m_importer.create_metadata(metadata)
    m_importer.run()


def main_silent() -> None:
    if args.finder == 'thredds':
        m_finder = ThreddsFinder()
    else:
        raise Exception(f'Unknown finder: {args.finder}')

    file = m_finder.start_silent(args)

    try:
        if args.parser == 'netcdf':
            m_parser = NetcdfParser(file)
        else:
            raise Exception(f'Unknown parser: {args.parser}')

        imports = m_parser.start_silent(args)

        option_ignore_zero_dups = 'ignore_zero_dups' in args.option if type(args.option) is list else False
        option_ignore_dups = 'ignore_dups' in args.option if type(args.option) is list else False
        m_importer = SplashbackImporter(os.environ['SPLASHBACK_API_KEY'], args.pool_id, imports,
                                        ignore_zero_dups=option_ignore_zero_dups, ignore_dups=option_ignore_dups)
        check_results = m_importer.check()
        metadata = m_parser.start_metadata_silent(check_results, args)

    finally:
        # Cleanup
        file.close()

    line_len = shutil.get_terminal_size()[0]
    for name, current, count in m_importer.create_metadata(metadata):
        end = '\n' if current == count else '\r'
        print(f'{name} ({current}/{count})'.ljust(line_len), end=end)

    result = m_importer.run()
    print(f'Imported {result["imported_sample_count"]} samples,'
          f' {result["imported_variant_count"]} variants and'
          f' {result["imported_value_count"]} values')


if __name__ == '__main__':
    # Setup argument parser
    parser = ArgumentParser()
    parser.add_argument('-i', '--interactive', action='store_true',
                        help='Enable interactive mode, ignoring other CLI arguments.')

    parser.add_argument('--pool-id', type=str,
                        help='Splashback Pool ID to integrate.')
    parser.add_argument('-f', '--finder', type=str, choices=['thredds'],
                        help='Finder to locate and fetch data.')
    parser.add_argument('-p', '--parser', type=str, choices=['netcdf'],
                        help='Parser to read fetched data.')
    parser.add_argument('-o', '--option', type=str, action='append',
                        help='Additional options.')

    args_no_help = [a for a in sys.argv if a != '-h' and a != '--help']
    current_args = parser.parse_known_args(args_no_help)[0]

    # Add finders
    is_finder_thredds = not current_args.interactive and current_args.finder == 'thredds'
    parser_group = parser.add_argument_group(title='Finder: thredds',
                                             description='Work with a THREDDS data source.')
    parser_group.add_argument('--thredds-dataset', type=str, required=is_finder_thredds,
                              help='THREDDS Dataset ID to download.')
    parser_group.add_argument('--thredds-service', type=str, required=is_finder_thredds,
                              help='THREDDS Service name to use for dataset download.')

    # Add parsers
    is_parser_netcdf = not current_args.interactive and current_args.parser == 'netcdf'
    parser_group = parser.add_argument_group(title='Parser: netcdf',
                                             description='Read NetCDF files.')
    parser_group.add_argument('--netcdf-mapping', type=str, required=is_parser_netcdf,
                              help='Field mapping file for the NetCDF parser.')

    args = parser.parse_args()

    if args.interactive:
        main_interactive()
    else:
        main_silent()
