from __future__ import annotations

from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import Union
from xml.etree import ElementTree

import requests

namespaces = {'': 'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0'}


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
                ThreddsDataset(self.server, dataset_id, self, dataset_size, dataset_size_units, dataset_date))


class ThreddsDataset(ThreddsServerAccessor):
    def __init__(self, server: ThreddsServer, dataset_id: str, catalog: ThreddsCatalog, size: float, size_units: str,
                 date: datetime):
        super().__init__(server)
        self._id = dataset_id
        self._catalog = catalog
        self._size = size
        self._size_units = size_units
        self._date = date

    @property
    def id(self) -> str:
        return self._id

    @property
    def catalog(self) -> ThreddsCatalog:
        return self._catalog

    @property
    def size(self) -> float:
        return self._size

    @property
    def size_units(self) -> str:
        return self._size_units

    @property
    def date(self) -> datetime:
        return self._date

    def download(self, service: ThreddsService) -> str:
        with requests.get(self.server.host + service.base + self.id, stream=True) as r:
            r.raise_for_status()
            with NamedTemporaryFile() as f:
                file_name = f.name
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return file_name
