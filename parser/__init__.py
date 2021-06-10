from argparse import Namespace
from pathlib import Path
from typing import List, Set, Dict, Tuple

from splashback_data.model.import_results import ImportResults
from splashback_data.model.laboratory_object import LaboratoryObject
from splashback_data.model.model_import import ModelImport
from splashback_data.model.parameter_object import ParameterObject
from splashback_data.model.program_object import ProgramObject
from splashback_data.model.quality_object import QualityObject
from splashback_data.model.sample_variant_type_object import SampleVariantTypeObject
from splashback_data.model.sampling_method_object import SamplingMethodObject
from splashback_data.model.site_object import SiteObject

import listutils


class ParsedMetadata:
    def __init__(self):
        self.__sites: List[SiteObject] = []
        self.__programs: List[ProgramObject] = []
        self.__variant_types: List[SampleVariantTypeObject] = []
        self.__parameters: List[ParameterObject] = []
        self.__laboratories: List[LaboratoryObject] = []
        self.__sampling_methods: List[SamplingMethodObject] = []
        self.__qualities: List[QualityObject] = []
        self.__lookups: Dict[str, Set[str]] = {
            'sites': set(),
            'programs': set(),
            'parameters': set(),
            'laboratories': set(),
            'sampling_methods': set(),
            'qualities': set()
        }

    def add_site(self, obj: SiteObject, lookup_key: str = None):
        obj_idx = listutils.add_unique_to_list(self.__sites, ['name', 'location'], obj)
        if lookup_key is not None:
            self.add_lookup('sites', lookup_key, obj_idx)

    @property
    def sites(self) -> List[SiteObject]:
        return self.__sites

    def add_program(self, obj: ProgramObject, lookup_key: str = None):
        obj_idx = listutils.add_unique_to_list(self.__programs, ['name'], obj)
        if lookup_key is not None:
            self.add_lookup('programs', lookup_key, obj_idx)

    @property
    def programs(self) -> List[ProgramObject]:
        return self.__programs

    def add_variant_type(self, obj: SampleVariantTypeObject):
        listutils.add_unique_to_list(self.__variant_types, ['name'], obj)

    @property
    def variant_types(self) -> List[SampleVariantTypeObject]:
        return self.__variant_types

    def add_parameter(self, obj: ParameterObject, lookup_key: str = None):
        obj_idx = listutils.add_unique_to_list(self.__parameters, ['name', 'unit'], obj)
        if lookup_key is not None:
            self.add_lookup('parameters', lookup_key, obj_idx)

    @property
    def parameters(self) -> List[ParameterObject]:
        return self.__parameters

    def add_laboratory(self, obj: LaboratoryObject, lookup_key: str = None):
        obj_idx = listutils.add_unique_to_list(self.__laboratories, ['name'], obj)
        if lookup_key is not None:
            self.add_lookup('laboratories', lookup_key, obj_idx)

    @property
    def laboratories(self) -> List[LaboratoryObject]:
        return self.__laboratories

    def add_sampling_method(self, obj: SamplingMethodObject, lookup_key: str = None):
        obj_idx = listutils.add_unique_to_list(self.__sampling_methods, ['name'], obj)
        if lookup_key is not None:
            self.add_lookup('sampling_methods', lookup_key, obj_idx)

    @property
    def sampling_methods(self) -> List[SamplingMethodObject]:
        return self.__sampling_methods

    def add_quality(self, obj: QualityObject, lookup_key: str = None):
        obj_idx = listutils.add_unique_to_list(self.__qualities, ['name'], obj)
        if lookup_key is not None:
            self.add_lookup('qualities', lookup_key, obj_idx)

    @property
    def qualities(self) -> List[QualityObject]:
        return self.__qualities

    def add_lookup(self, obj_type: str, key: str, idx: int):
        self.__lookups[obj_type].add(f'{key}:{idx}')

    def get_lookups(self, obj_type: str) -> List[Tuple[str, int]]:
        def map_lookup(lookup: str) -> Tuple[str, int]:
            parts = lookup.rsplit(':', 1)
            return parts[0], int(parts[1])

        return list(map(map_lookup, self.__lookups[obj_type]))


class BaseParser:
    def __init__(self, path: Path):
        self._path: Path = path
        self._imports: List[ModelImport] = []

    def start_interactive(self) -> List[ModelImport]:
        return self._start_interactive()

    def _start_interactive(self) -> List[ModelImport]:
        raise NotImplementedError()

    def start_silent(self, args: Namespace) -> List[ModelImport]:
        ignore_zero_dups = 'ignore_zero_dups' in args.option if type(args.option) is list else False
        ignore_dups = 'ignore_dups' in args.option if type(args.option) is list else False

        self._imports = self._start_silent(args)
        if ignore_zero_dups:
            self._ignore_zero_dups()
        if ignore_dups:
            self._ignore_dups()
        return self._imports

    def _start_silent(self, args: Namespace) -> List[ModelImport]:
        raise NotImplementedError()

    def start_metadata_interactive(self, results: ImportResults, metadata: ParsedMetadata = None) -> ParsedMetadata:
        raise NotImplementedError()

    def start_metadata_silent(self, results: ImportResults, args: Namespace, metadata: ParsedMetadata = None) \
            -> ParsedMetadata:
        raise NotImplementedError()

    @staticmethod
    def _compare_models(a: ModelImport, b: ModelImport):
        return a['site_code'] == b['site_code'] \
               and a['date'] == b['date'] \
               and a['program'] == b['program'] \
               and a['variant_type'] == b['variant_type'] \
               and a['variant_date_time'] == b['variant_date_time'] \
               and a['variant_value'] == b['variant_value'] \
               and a['variant_comment'] == b['variant_comment'] \
               and a['parameter'] == b['parameter']

    def _ignore_zero_dups(self) -> None:
        def is_zero_model(mdl: ModelImport):
            return float(mdl['value']) == 0. and float(mdl['variant_value']) == 0.

        zero_dup_idxs = set()
        for idx_a, mdl_a in enumerate(self._imports):
            for idx_b, mdl_b in enumerate(self._imports):
                # Only check previous entries
                if idx_b >= idx_a:
                    break

                # Compare
                if self._compare_models(mdl_a, mdl_b):
                    if is_zero_model(mdl_a):
                        zero_dup_idxs.add(idx_a)
                    if is_zero_model(mdl_b):
                        zero_dup_idxs.add(idx_b)

        self._imports = [mdl for idx, mdl in enumerate(self._imports) if idx not in zero_dup_idxs]

    def _ignore_dups(self) -> None:
        dup_idxs = set()
        for idx_a, mdl_a in enumerate(self._imports):
            for idx_b, mdl_b in enumerate(self._imports):
                # Only check previous entries
                if idx_b >= idx_a:
                    break

                # Compare
                if self._compare_models(mdl_a, mdl_b):
                    dup_idxs.add(idx_b)

        self._imports = [mdl for idx, mdl in enumerate(self._imports) if idx not in dup_idxs]