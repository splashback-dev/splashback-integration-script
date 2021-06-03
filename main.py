from thredds import *

server_host = 'http://thredds.aodn.org.au'


def select_dataset(catalog: ThreddsCatalog) -> Union[str, None]:
    catalog.load()

    index = 0
    for c in catalog.children:
        index += 1
        print('[' + str(index) + ']', c.id)
    for d in catalog.datasets:
        index += 1
        print('[' + str(index) + ']', d.id, '(Size: '+str(d.size)+d.size_units+', Date: '+d.date.isoformat()+')')

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
        print('Invalid command')
        return

    if cmd_index < 1 or cmd_index > index:
        print('Invalid index')
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
            print('Invalid command')
            return

        if 0 < service_cmd_index <= service_index:
            service = catalog.server.services[service_cmd_index - 1]

    # Download dataset
    print('Downloading dataset', cmd_dataset.id, '...')
    file_name = cmd_dataset.download(service)
    print('Dataset downloaded to', file_name)
    return file_name


def main():
    print('Connecting to', server_host, '...')
    server = ThreddsServer(server_host)
    print('Found catalog', server.name, 'v' + server.version)
    print('Found services', ', '.join([s.name for s in server.services]))

    file_name = select_dataset(server.catalog)
    if file_name is None:
        return
    print(file_name)


if __name__ == '__main__':
    main()
