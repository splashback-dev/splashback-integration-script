import json
import os
import sys
from typing import Union
from typing.io import IO

from argparse import ArgumentParser
from splashback import SplashbackImporter
from thredds import ThreddsServer, ThreddsCatalog, ThreddsDataset

# Setup THREDDS server
server_host = 'http://thredds.aodn.org.au'

# Setup Splashback API configuration
splashback_host = 'https://api.splashback.io'


def select_dataset(catalog: ThreddsCatalog) -> Union[IO, None]:
    catalog.load()

    index = 0
    for c in catalog.children:
        index += 1
        print('[' + str(index) + ']', c.id)
    for d in catalog.datasets:
        index += 1
        print('[' + str(index) + ']', d.id,
              '(Size: ' + str(d.size) + d.size_units + ', Date: ' + d.date.isoformat() + ')')

    cmd = input('>> ').strip().lower()
    if cmd == 'q':
        # Quit
        print('Quiting ...')
        return
    if cmd == 'b':
        # Back
        if catalog.parent is None:
            print('No parent catalog. Quiting ...')
            return
        return select_dataset(catalog.parent)
    if cmd == 'r':
        # Refresh
        return select_dataset(catalog)

    try:
        cmd_index = int(cmd)
    except ValueError:
        print('Invalid command', file=sys.stderr)
        return

    if cmd_index < 1 or cmd_index > index:
        print('Invalid index', file=sys.stderr)
        return

    if cmd_index <= len(catalog.children):
        # Catalog selected
        cmd_catalog = catalog.children[cmd_index - 1]
        print('Loading catalog', cmd_catalog.id, '...')
        return select_dataset(cmd_catalog)

    # Dataset selected
    cmd_index -= len(catalog.children)
    cmd_dataset = catalog.datasets[cmd_index - 1]

    # Select service
    service = None
    while service is None:
        print('Select a service ...')
        service_index = 0
        for s in catalog.server.services:
            service_index += 1
            print('[' + str(service_index) + ']', s.name)

        service_cmd = input('>> ').strip().lower()
        if service_cmd == 'b':
            return select_dataset(catalog)

        try:
            service_cmd_index = int(service_cmd)
        except ValueError:
            print('Invalid command', file=sys.stderr)
            return

        if 0 < service_cmd_index <= service_index:
            service = catalog.server.services[service_cmd_index - 1]

    # Download dataset
    print('Downloading dataset', cmd_dataset.id, '...')
    file = cmd_dataset.download(service)
    print('Dataset downloaded')
    return file


def import_dataset(file: IO) -> None:
    # Read Splashback API key
    print('Enter your Splashback API key ...')
    api_key = input('>> ').strip()

    # Enter Splashback Pool ID
    print('Enter your Splashback Pool ID ...')
    pool_id = input('>> ').strip()

    # Load dataset
    importer = SplashbackImporter(splashback_host, api_key, pool_id, file)


def main_interactive() -> None:
    print('Connecting to', server_host, '...')
    server = ThreddsServer(server_host)
    print('Found catalog', server.name, 'v' + server.version)
    print('Found services', ', '.join([s.name for s in server.services]))

    # Select dataset file
    file = select_dataset(server.catalog)
    if file is None:
        return

    # Load dataset file
    try:
        import_dataset(file)
    finally:
        # Cleanup
        file.close()


def main() -> None:
    # Load THREDDS server
    thredds_server = ThreddsServer(server_host)

    # Get THREDDS service
    thredds_service = thredds_server.find_service(args.service)
    if thredds_service is None:
        print('Service', args.service, 'not found', file=sys.stderr)
        return

    # Open THREDDS dataset
    thredds_dataset = ThreddsDataset(thredds_server, args.dataset)
    file = thredds_dataset.download(thredds_service)

    # Read mapping file
    with open(args.mapping, 'r') as m:
        mapping = json.load(m)

    # Import to Splashback
    try:
        importer = SplashbackImporter(splashback_host, os.environ['SPLASHBACK_API_KEY'], args.pool_id, file)
        imports = [r for r in importer.parse_rows(mapping)]
        importer.check(imports)
    finally:
        # Cleanup
        file.close()


if __name__ == '__main__':
    # Setup argument parser
    parser = ArgumentParser()
    parser.add_argument('-i', '--interactive',
                        help='Enable interactive mode. All other options will be ignored.', action='store_true')
    parser.add_argument('-d', '--dataset', type=str,
                        help='THREDDS Dataset ID to download.')
    parser.add_argument('-s', '--service', type=str,
                        help='THREDDS Service name to use for dataset download.')
    parser.add_argument('-p', '--pool-id', type=str,
                        help='Splashback Pool ID to integrate.')
    parser.add_argument('-m', '--mapping', type=str,
                        help='Splashback importer field mapping file.')
    args = parser.parse_args()

    if args.interactive:
        main_interactive()
    else:
        main()
