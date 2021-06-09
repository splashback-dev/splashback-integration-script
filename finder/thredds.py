from __future__ import annotations

import sys
from argparse import Namespace
from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import Union
from typing.io import IO
from xml.etree import ElementTree

import requests

from finder import BaseFinder

namespaces = {'': 'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0'}

# Setup THREDDS server
server_host = 'http://thredds.aodn.org.au'


class ThreddsFinder(BaseFinder):
    def __init__(self):
        super().__init__()

    def start_interactive(self) -> IO:
        print('Connecting to', server_host, '...')
        server = ThreddsServer(server_host)
        print('Found catalog', server.name, 'v' + server.version)
        print('Found services', ', '.join([s.name for s in server.services]))

        # Select dataset file
        file = self._select_dataset(server.catalog)
        if file is None:
            raise Exception('No file selected')
        return file

    def _select_dataset(self, catalog: ThreddsCatalog) -> Union[IO, None]:
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
            return self._select_dataset(catalog.parent)
        if cmd == 'r':
            # Refresh
            return self._select_dataset(catalog)

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
            return self._select_dataset(cmd_catalog)

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
                return self._select_dataset(catalog)

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

    def start_silent(self, args: Namespace) -> IO:
        # Load THREDDS server
        thredds_server = ThreddsServer(server_host)

        # Get THREDDS service
        thredds_service = thredds_server.find_service(args.thredds_service)
        if thredds_service is None:
            raise Exception('Service', args.thredds_service, 'not found')

        # Open THREDDS dataset
        thredds_dataset = ThreddsDataset(thredds_server, args.thredds_dataset)
        return thredds_dataset.download(thredds_service)


class ThreddsServer:
    def __init__(self, host: str):
        self._host = host
        self._services = []
        self._load()

    def get_xml(self, path: str):
        r = requests.get(self._host + path)
        return ElementTree.fromstring(r.content)

    def _load(self) -> None:
        root_xml = self.get_xml('/thredds/catalog/catalog.xml')
        self._name = root_xml.get('name')
        self._version = root_xml.get('version')

        for service_xml in root_xml.findall('./service/service', namespaces=namespaces):
            name = service_xml.get('name')
            base = service_xml.get('base')
            self._services.append(ThreddsService(self, name, base))

        self._catalog = ThreddsCatalog(self, '')

    @property
    def host(self) -> str:
        return self._host

    @property
    def name(self) -> str:
        return self._name

    @property
    def version(self) -> str:
        return self._version

    @property
    def services(self) -> [ThreddsService]:
        return self._services

    @property
    def catalog(self) -> ThreddsCatalog:
        return self._catalog

    def find_service(self, name) -> Union[ThreddsService, None]:
        for s in self._services:
            if s.name == name:
                return s
        return


class ThreddsServerAccessor:
    def __init__(self, server: ThreddsServer):
        if server is None:
            raise Exception('Server cannot be None.')
        self.__server = server

    @property
    def server(self) -> ThreddsServer:
        return self.__server


class ThreddsService(ThreddsServerAccessor):
    _name = ''
    _base = ''

    def __init__(self, server: ThreddsServer, name: str, base: str):
        super().__init__(server)
        self._name = name
        self._base = base

    @property
    def name(self) -> str:
        return self._name

    @property
    def base(self) -> str:
        return self._base


class ThreddsCatalog(ThreddsServerAccessor):
    def __init__(self, server: ThreddsServer, catalog_id: str, parent: ThreddsCatalog = None):
        super().__init__(server)
        self._id = catalog_id
        self._parent = parent
        self._children = []
        self._datasets = []

    @property
    def parent(self) -> ThreddsCatalog:
        return self._parent

    @property
    def children(self) -> [ThreddsCatalog]:
        return self._children

    @property
    def datasets(self) -> [ThreddsDataset]:
        return self._datasets

    @property
    def id(self) -> str:
        return self._id

    def load(self) -> None:
        self._children = []
        self._datasets = []

        root_xml = self.server.get_xml('/thredds/catalog/' + self._id + '/catalog.xml')

        for catalogRef_xml in root_xml.findall('.//catalogRef', namespaces=namespaces):
            catalog_id = catalogRef_xml.get('ID')
            self._children.append(ThreddsCatalog(self.server, catalog_id, parent=self))

        for dataset_xml in root_xml.findall('./dataset/dataset', namespaces=namespaces):
            dataset_id = dataset_xml.get('ID')
            dataset_size = float(dataset_xml.find('dataSize', namespaces=namespaces).text)
            dataset_size_units = dataset_xml.find('dataSize', namespaces=namespaces).get('units')
            dataset_date = datetime.fromisoformat(dataset_xml.find('date', namespaces=namespaces).text[0:-1])
            self._datasets.append(
                ThreddsDataset(self.server, dataset_id, dataset_size, dataset_size_units, dataset_date))


class ThreddsDataset(ThreddsServerAccessor):
    def __init__(self, server: ThreddsServer, dataset_id: str, size: float = 0., size_units: str = '',
                 date: datetime = None):
        super().__init__(server)
        self._id = dataset_id
        self._size = size
        self._size_units = size_units
        self._date = date

    @property
    def id(self) -> str:
        return self._id

    @property
    def size(self) -> float:
        return self._size

    @property
    def size_units(self) -> str:
        return self._size_units

    @property
    def date(self) -> datetime:
        return self._date

    def download(self, service: ThreddsService) -> IO:
        file = NamedTemporaryFile()
        with requests.get(self.server.host + service.base + self.id, stream=True) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=8192):
                file.write(chunk)
        return file
