import json
import re
from argparse import Namespace
from pathlib import Path
from typing import List, Generator, Any, Type

from splashback_data.model.import_check_stage import ImportCheckStage
from splashback_data.model.import_check_status import ImportCheckStatus
from splashback_data.model.import_results import ImportResults
from splashback_data.model.laboratory_object import LaboratoryObject
from splashback_data.model.model_import import ModelImport
from splashback_data.model.parameter_object import ParameterObject
from splashback_data.model.program_object import ProgramObject
from splashback_data.model.quality_object import QualityObject
from splashback_data.model.sample_variant_type_object import SampleVariantTypeObject
from splashback_data.model.sampling_method_object import SamplingMethodObject
from splashback_data.model.site_object import SiteObject

import timeconverters
from parser import BaseParser, ParsedMetadata


class JsonParser(BaseParser):
    def __init__(self, path: Path):
        super().__init__(path)

        with open(path, 'r') as j:
            self._json = json.load(j)
        self._mapping = {}

    def _start_interactive(self) -> List[ModelImport]:
        raise NotImplementedError()

    def _start_silent(self, args: Namespace) -> List[ModelImport]:
        # Read mapping file
        with open(args.json_mapping, 'r') as m:
            self._mapping = json.load(m)

        if args.verbose:
            print('JSON', self._json)
            print('Mapping', repr(self._mapping))

        self._imports = [r for r in self._parse_imports()]
        return self._imports

    def start_metadata_interactive(self, results: ImportResults, metadata: ParsedMetadata = None) -> ParsedMetadata:
        raise NotImplementedError()

    def start_metadata_silent(self, results: ImportResults, args: Namespace,
                              metadata: ParsedMetadata = None) -> ParsedMetadata:
        if metadata is None:
            metadata = ParsedMetadata()

        for message in results['messages']:
            # Filter status
            if message['status'] != ImportCheckStatus(1):
                continue

            model_import = self._imports[message['index']]

            if message['stage'] == ImportCheckStage(1):
                if 'SiteName' in message['fields'] and 'SiteCode' in message['fields']:
                    site = self._get_metadata(SiteObject, 'site', model_import)
                    metadata.add_site(site, model_import['site_code'])

                elif 'Program' in message['fields']:
                    program = self._get_metadata(ProgramObject, 'program', model_import)
                    metadata.add_program(program, model_import['program'])

                elif 'VariantType' in message['fields']:
                    variant_type = self._get_metadata(SampleVariantTypeObject, 'variant_type', model_import)
                    metadata.add_variant_type(variant_type)

                elif 'Parameter' in message['fields']:
                    parameter = self._get_metadata(ParameterObject, 'parameter', model_import)
                    metadata.add_parameter(parameter, model_import['parameter'])

                elif 'Laboratory' in message['fields']:
                    laboratory = self._get_metadata(LaboratoryObject, 'laboratory', model_import)
                    metadata.add_laboratory(laboratory, model_import['laboratory'])

                elif 'SamplingMethod' in message['fields']:
                    sampling_method = self._get_metadata(SamplingMethodObject, 'sampling_method', model_import)
                    metadata.add_sampling_method(sampling_method, model_import['sampling_method'])

                elif 'Quality' in message['fields']:
                    quality = self._get_metadata(QualityObject, 'quality', model_import)
                    metadata.add_quality(quality, model_import['quality'])

        return metadata

    @staticmethod
    def _find_by_path(obj: Any, m_keys: List[str]) -> Any:
        for m_key in m_keys:
            # Lists need integer key
            if isinstance(obj, list):
                m_key = int(m_key)
            obj = obj[m_key]
        return obj

    def _parse_imports(self) -> Generator[ModelImport, None, None]:
        row_list = self._json
        import_path = self._mapping['import_path']
        import_path_keys = import_path.split('.')
        row_list = self._find_by_path(row_list, import_path_keys)
        if not isinstance(row_list, list):
            raise Exception(f'Invalid type parsed for import path: {import_path}')

        # Iterate rows in import_path
        for row_index in range(len(row_list)):
            yield self._get_import(row_index)

    def _get_import(self, row_index: int) -> ModelImport:
        return ModelImport(**{k: self._get_field(v, row_index=row_index)
                              for k, v in self._mapping['templates']['import'].items()})

    def _get_metadata(self, type: Type, template_name: str, model_import: ModelImport) -> Any:
        return type(**{k: self._get_field(v, model_import=model_import)
                       for k, v in self._mapping['templates'][template_name].items()})

    def _get_field(self, field: str, row_index: int = None, model_import: ModelImport = None) -> Any:
        field_parts = field.split('|')

        exception = None
        for field_part in field_parts:
            try:
                return self._get_field_value(field_part, row_index=row_index, model_import=model_import)
            except Exception as e:
                exception = e

        raise Exception(f'Failed to get field: {exception}')

    def _get_field_value(self, field: str, row_index: int = None, model_import: ModelImport = None) -> Any:
        # Strip field
        field = field.strip()
        if field == '':
            return ''

        # Get subfields
        field = re.sub(r'\((.*)\)', lambda match: self._get_field(match.group(1), model_import=model_import), field)

        # Parse args
        accessor, *args_list = field.split(' ')
        field_type = 'str'
        if args_list[-1][0] == '!':
            field_type = args_list.pop(-1)[1:]
        args = ' '.join(args_list)

        # JSON <path_name>: Get a JSON value
        if accessor == 'JSON':
            if len(args_list) != 1:
                raise Exception('Expected one argument for accessor JSON')

            field_value = self._json
            keys = args_list[0].split('.')

            # If leading key is $, reference current row
            if keys[0] == '$':
                if row_index is None:
                    raise Exception('Cannot reference current row from this field.')

                keys.pop(0)
                import_path_keys = self._mapping['import_path'].split('.')
                field_value = self._find_by_path(field_value, import_path_keys)
                field_value = field_value[row_index]

            field_value = self._find_by_path(field_value, keys)

        # CONST <values...>: Constant value
        elif accessor == 'CONST':
            field_value = args

        else:
            raise Exception('Unknown accessor: %s' % accessor)

        if field_value is None:
            raise Exception('Field value not set')

        # Convert to correct type and return
        field_type_parts = field_type.split(':')

        if field_type_parts[0] == 'str':
            if isinstance(field_value, float):
                field_str_value = f'{field_value:.30f}'
            else:
                field_str_value = str(field_value)

            if len(field_type_parts) == 1:
                return field_str_value
            return field_str_value[0:int(field_type_parts[1])]

        elif field_type_parts[0] == 'float':
            return float(field_value)

        elif field_type_parts[0] == 'datetime':
            dt = None
            if field_type_parts[1] == 'days_since_1950':
                dt = timeconverters.convert_days_since_1950_to_datetime(float(field_value))
            elif field_type_parts[1] == 'bom_date_time_full':
                dt = timeconverters.convert_bom_date_time_full_to_datetime(field_value)

            if dt is None:
                raise Exception('Invalid datetime value type parser')

            if len(field_type_parts) > 2:
                if field_type_parts[2] == 'json':
                    return dt.strftime('%Y-%m-%dT%H:%M:%S')
                raise Exception('Invalid datetime value formatter')

            return dt

        raise Exception('Unknown field type: %s' % field_type)
