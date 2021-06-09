import json
import re
from argparse import Namespace
from typing import Generator, List, Type, Any
from typing.io import IO

import numpy
from netCDF4 import Dataset, Variable
from numpy import ndenumerate
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


class NetcdfParser(BaseParser):
    def __init__(self, file: IO):
        super().__init__(file)

        self._dataset = Dataset(self._file.name, 'r')
        self._mapping = {}

    def start_silent(self, args: Namespace) -> List[ModelImport]:
        # Read mapping file
        with open(args.netcdf_mapping, 'r') as m:
            self._mapping = json.load(m)

        self._imports = [r for r in self._parse_imports()]
        return self._imports

    def start_metadata_silent(self, results: ImportResults, args: Namespace) -> ParsedMetadata:
        metadata = ParsedMetadata()

        for message in results['messages']:
            # Filter status
            if message['status'] != ImportCheckStatus(1):
                continue

            model_import = self._imports[message['index']]

            if message['stage'] != ImportCheckStage(1):
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

    def _parse_imports(self) -> Generator[ModelImport, None, None]:
        # Iterate each parameter
        for parameter_var in [self._dataset.variables[v] for v in self._mapping['parameters']]:

            # Iterate every combination of indices
            for dim_idxs, val in ndenumerate(parameter_var[:]):

                # Filter out-of-bounds values
                if val < parameter_var.valid_min or val >= parameter_var.valid_max:
                    continue

                yield self._get_import(parameter_var, dim_idxs, val)

    def _get_import(self, parameter_var: Variable, dim_idxs: List[str], val: float) -> ModelImport:
        return ModelImport(**{k: self._get_field(v, parameter_var=parameter_var, dim_idxs=dim_idxs, val=val)
                              for k, v in self._mapping['templates']['import'].items()})

    def _get_metadata(self, type: Type, template_name: str, model_import: ModelImport) -> Any:
        return type(**{k: self._get_field(v, model_import=model_import)
                       for k, v in self._mapping['templates'][template_name].items()})

    def _get_field(self, field: str,
                   parameter_var: Variable = None, dim_idxs: List[str] = None, val: float = None,
                   model_import: ModelImport = None) -> Any:
        # Get kwargs as dict
        kwargs = dict(locals())
        del kwargs['self']
        del kwargs['field']

        # Strip field
        field = field.strip()
        if field == '':
            return ''

        # Get subfields
        field = re.sub(r'\((.*)\)', lambda match: self._get_field(match.group(1), **kwargs), field)

        # Parse args
        accessor, *args_list = field.split(' ')
        field_type = 'str'
        if args_list[-1][0] == '!':
            field_type = args_list.pop(-1)[1:]
        args = ' '.join(args_list)
        field_value = None

        # ATTR <attr_name>: Get a global attribute value
        if accessor == 'ATTR':
            if len(args_list) != 1:
                raise Exception('Expected one argument for accessor ATTR')

            field_value = self._dataset.getncattr(args_list[0])

        # VARATTR <var_name> <attr_name>: Get a variable attribute value
        elif accessor == 'VARATTR':
            if len(args_list) != 2:
                raise Exception('Expected two arguments for accessor VARATTR')

            var: Variable = self._dataset.variables[args_list[0]]
            field_value = var.getncattr(args_list[1])

        # CONST <values...>: Constant value
        elif accessor == 'CONST':
            field_value = args

        # PARAM name|value: Get parameter name or value (import template only)
        elif accessor == 'PARAM':
            if parameter_var is None or val is None:
                raise Exception('Cannot use accessor PARAM in this context')

            if args == 'name':
                field_value = parameter_var.name
            if args == 'value':
                field_value = val

        # VAR <var_name>: Get variable value
        elif accessor == 'VAR':
            if dim_idxs is None or parameter_var is None:
                raise Exception('Cannot use accessor VAR in this context')

            var: Variable = self._dataset.variables[args_list[0]]
            var_idxs = [dim_idxs[idx] for idx, dim in enumerate(parameter_var.dimensions) if dim in var.dimensions]
            field_value = var[tuple(var_idxs)].data

        # FIELD <field_name>: Get import field (metadata templates only)
        elif accessor == 'FIELD':
            if len(args_list) != 1:
                raise Exception('Expected one argument for accessor FIELD')
            if model_import is None:
                raise Exception('Cannot use accessor FIELD in this context')

            field_value = model_import[args_list[0]]

        else:
            raise Exception('Unknown accessor: %s' % accessor)

        if field_value is None:
            raise Exception('Field value not set')

        # Convert to correct type and return
        if field_type == 'str':
            if isinstance(field_value, float) or isinstance(field_value, numpy.ndarray):
                return f'{field_value:.8f}'
            return str(field_value)
        elif field_type == 'float':
            return float(field_value)

        field_type_parts = field_type.split(':')
        if field_type_parts[0] == 'datetime':
            dt = None
            if field_type_parts[1] == 'days_since_1950':
                dt = timeconverters.convert_days_since_1950_to_datetime(float(field_value))

            if dt is None:
                raise Exception('Invalid datetime value type parser')

            if len(field_type_parts) > 2:
                if field_type_parts[2] == 'json':
                    return dt.strftime('%Y-%m-%dT%H:%M:%S')
                raise Exception('Invalid datetime value formatter')

            return dt

        raise Exception('Unknown field type: %s' % field_type)
