from typing import List
from typing.io import IO

import splashback_data
from netCDF4 import Dataset, Variable, Dimension
from numpy import ndenumerate
from splashback_data.api import imports_api
from splashback_data.models import ModelImport

import timeconverters


class SplashbackImporter:
    def __init__(self, host: str, api_key: str, pool_id: str, file: IO):
        self._configuration = splashback_data.Configuration(
            host=host + '/data'
        )
        self._configuration.api_key['api-key'] = api_key
        self._configuration.api_key_prefix['api-key'] = 'API-Key'

        self._pool_id = pool_id

        self._dataset = Dataset(file.name, 'r')
        print(self._dataset)

    def parse_rows(self, mapping: dict) -> List[ModelImport]:
        # Iterate each parameter
        for parameter_var in [self._dataset.variables[v] for v in mapping['parameters']]:

            # Iterate every combination of indices
            for dim_idxs, val in ndenumerate(parameter_var[:]):

                # Filter out-of-bounds values
                if val < parameter_var.valid_min or val >= parameter_var.valid_max:
                    continue

                yield self._get_import(mapping, parameter_var, dim_idxs, val)

    def _get_import(self, mapping: dict, parameter_var: Variable, dim_idxs: List[str], val: float) -> ModelImport:
        return ModelImport(
            **{k: self._get_import_field(v, parameter_var, dim_idxs, val) for k, v in mapping['template'].items()})

    def _get_import_field(self, field: str, parameter_var: Variable, dim_idxs: List[str], val: float) -> str:
        field = field.strip()
        if field == '':
            return ''

        accessor, *args_list = field.split(' ')
        args = ' '.join(args_list)
        if accessor == 'ATTR':
            return str(self._dataset.getncattr(args))
        if accessor == 'CONST':
            return args
        if accessor == 'PARAM':
            if args == 'name':
                return parameter_var.name
            if args == 'value':
                return str(val)
        if accessor == 'VAR':
            var: Variable = self._dataset.variables[args_list[0]]
            var_idxs = [dim_idxs[i] for i, d in enumerate(parameter_var.dimensions) if d in var.dimensions]
            return str(var[tuple(var_idxs)])
        else:
            return ''

    def check(self, rows: List[ModelImport]) -> None:
        with splashback_data.ApiClient(self._configuration) as client:
            instance = imports_api.ImportsApi(client)

            try:
                response = instance.api_imports_check_pool_id_post(model_import=rows, pool_id=int(self._pool_id))
                print(response)
            except splashback_data.ApiException as e:
                print('Exception when calling: %s\n' % e)
