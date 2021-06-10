from typing import Generator, List, Tuple

import splashback_data
from splashback_data.api import imports_api, sites_api, site_lookups_api, programs_api, program_lookups_api, \
    sample_variant_types_api, parameters_api, parameter_lookups_api, laboratories_api, laboratory_lookups_api, \
    sampling_methods_api, sampling_method_lookups_api, qualities_api, quality_lookups_api
from splashback_data.model.import_check_stage import ImportCheckStage
from splashback_data.model.import_check_status import ImportCheckStatus
from splashback_data.model.import_results import ImportResults
from splashback_data.model.import_run_result import ImportRunResult
from splashback_data.model.lookup_object import LookupObject
from splashback_data.model.model_import import ModelImport

import listutils
from parser import ParsedMetadata

# Setup Splashback API configuration
splashback_host = 'https://api.splashback.io'


class SplashbackImporter:
    def __init__(self, api_key: str, pool_id: str):
        self._configuration = splashback_data.Configuration(
            host=splashback_host + '/data'
        )
        self._configuration.api_key['api-key'] = api_key
        self._configuration.api_key_prefix['api-key'] = 'API-Key'

        self._pool_id = pool_id

    @property
    def pool_id(self) -> int:
        return int(self._pool_id)

    def check(self, imports: List[ModelImport]) -> ImportResults:
        with splashback_data.ApiClient(self._configuration) as client:
            instance = imports_api.ImportsApi(client)

            results: ImportResults = instance.api_imports_check_pool_id_post(model_import=imports,
                                                                             pool_id=self.pool_id)

        return results

    def create_metadata(self, metadata: ParsedMetadata) -> Generator[Tuple[str, int, int], None, None]:
        with splashback_data.ApiClient(self._configuration) as client:
            # region Sites
            site_instance = sites_api.SitesApi(client)
            remote_sites = site_instance.api_sites_pool_id_get(pool_id=self.pool_id)
            for site_idx, site in enumerate(metadata.sites):
                remote_site = listutils.get_unique_value(remote_sites, ['name', 'location'], site)
                if remote_site is None:
                    remote_site = site_instance.api_sites_pool_id_post(site_object=site, pool_id=self.pool_id)

                site['id'] = remote_site['id']
                yield 'sites', site_idx + 1, len(metadata.sites)

            site_lookup_instance = site_lookups_api.SiteLookupsApi(client)
            site_lookups = metadata.get_lookups('sites')
            for lookup_idx, (key, idx) in enumerate(site_lookups):
                lookup = LookupObject(id=metadata.sites[idx]['id'], key=key, pool_id=self.pool_id)

                site_lookup_instance.api_site_lookups_pool_id_post(lookup_object=lookup, pool_id=self.pool_id)
                yield 'site lookups', lookup_idx + 1, len(site_lookups)
            # endregion

            # region Programs
            program_instance = programs_api.ProgramsApi(client)
            remote_programs = program_instance.api_programs_pool_id_get(pool_id=self.pool_id)
            for program_idx, program in enumerate(metadata.programs):
                remote_program = listutils.get_unique_value(remote_programs, ['name'], program)
                if remote_program is None:
                    remote_program = program_instance.api_programs_pool_id_post(program_object=program,
                                                                                pool_id=self.pool_id)

                program['id'] = remote_program['id']
                yield 'program', program_idx + 1, len(metadata.programs)

            program_lookup_instance = program_lookups_api.ProgramLookupsApi(client)
            program_lookups = metadata.get_lookups('programs')
            for lookup_idx, (key, idx) in enumerate(program_lookups):
                lookup = LookupObject(id=metadata.programs[idx]['id'], key=key, pool_id=self.pool_id)

                program_lookup_instance.api_program_lookups_pool_id_post(lookup_object=lookup, pool_id=self.pool_id)
                yield 'program lookups', lookup_idx + 1, len(program_lookups)
            # endregion

            # region Variant Types
            variant_type_instance = sample_variant_types_api.SampleVariantTypesApi(client)
            for variant_type_idx, variant_type in enumerate(metadata.variant_types):
                variant_type_instance.api_sample_variant_types_pool_id_post(sample_variant_type_object=variant_type,
                                                                            pool_id=self.pool_id)
                yield 'variant type', variant_type_idx + 1, len(metadata.variant_types)
            # endregion

            # region Parameters
            parameter_instance = parameters_api.ParametersApi(client)
            remote_parameters = parameter_instance.api_parameters_pool_id_get(pool_id=self.pool_id)
            for parameter_idx, parameter in enumerate(metadata.parameters):
                remote_parameter = listutils.get_unique_value(remote_parameters, ['name', 'unit'], parameter)
                if remote_parameter is None:
                    remote_parameter = parameter_instance.api_parameters_pool_id_post(parameter_object=parameter,
                                                                                      pool_id=self.pool_id)

                parameter['id'] = remote_parameter['id']
                yield 'parameter', parameter_idx + 1, len(metadata.parameters)

            parameter_lookup_instance = parameter_lookups_api.ParameterLookupsApi(client)
            parameter_lookups = metadata.get_lookups('parameters')
            for lookup_idx, (key, idx) in enumerate(parameter_lookups):
                lookup = LookupObject(id=metadata.parameters[idx]['id'], key=key, pool_id=self.pool_id)

                parameter_lookup_instance.api_parameter_lookups_pool_id_post(lookup_object=lookup, pool_id=self.pool_id)
                yield 'parameter lookups', lookup_idx + 1, len(parameter_lookups)
            # endregion

            # region Laboratories
            laboratory_instance = laboratories_api.LaboratoriesApi(client)
            remote_laboratories = laboratory_instance.api_laboratories_pool_id_get(pool_id=self.pool_id)
            for laboratory_idx, laboratory in enumerate(metadata.laboratories):
                remote_laboratory = listutils.get_unique_value(remote_laboratories, ['name'], laboratory)
                if remote_laboratory is None:
                    remote_laboratory = laboratory_instance.api_laboratories_pool_id_post(laboratory_object=laboratory,
                                                                                          pool_id=self.pool_id)

                laboratory['id'] = remote_laboratory['id']
                yield 'laboratory', laboratory_idx + 1, len(metadata.laboratories)

            laboratory_lookup_instance = laboratory_lookups_api.LaboratoryLookupsApi(client)
            laboratory_lookups = metadata.get_lookups('laboratories')
            for lookup_idx, (key, idx) in enumerate(laboratory_lookups):
                lookup = LookupObject(id=metadata.laboratories[idx]['id'], key=key, pool_id=self.pool_id)

                laboratory_lookup_instance.api_laboratory_lookups_pool_id_post(lookup_object=lookup,
                                                                               pool_id=self.pool_id)
                yield 'laboratory lookups', lookup_idx + 1, len(laboratory_lookups)
            # endregion

            # region Sampling Methods
            sampling_method_instance = sampling_methods_api.SamplingMethodsApi(client)
            remote_sampling_methods = sampling_method_instance.api_sampling_methods_pool_id_get(pool_id=self.pool_id)
            for sampling_method_idx, sampling_method in enumerate(metadata.sampling_methods):
                remote_sampling_method = listutils.get_unique_value(remote_sampling_methods, ['name'], sampling_method)
                if remote_sampling_method is None:
                    remote_sampling_method = sampling_method_instance.api_sampling_methods_pool_id_post(
                        sampling_method_object=sampling_method,
                        pool_id=self.pool_id)

                sampling_method['id'] = remote_sampling_method['id']
                yield 'sampling method', sampling_method_idx + 1, len(metadata.sampling_methods)

            sampling_method_lookup_instance = sampling_method_lookups_api.SamplingMethodLookupsApi(client)
            sampling_method_lookups = metadata.get_lookups('sampling_methods')
            for lookup_idx, (key, idx) in enumerate(sampling_method_lookups):
                lookup = LookupObject(id=metadata.sampling_methods[idx]['id'], key=key, pool_id=self.pool_id)

                sampling_method_lookup_instance.api_sampling_method_lookups_pool_id_post(lookup_object=lookup,
                                                                                         pool_id=self.pool_id)
                yield 'sampling method lookups', lookup_idx + 1, len(sampling_method_lookups)
            # endregion

            # region Qualities
            quality_instance = qualities_api.QualitiesApi(client)
            remote_qualities = quality_instance.api_qualities_pool_id_get(pool_id=self.pool_id)
            for quality_idx, quality in enumerate(metadata.qualities):
                remote_quality = listutils.get_unique_value(remote_qualities, ['name'], quality)
                if remote_quality is None:
                    remote_quality = quality_instance.api_qualities_pool_id_post(quality_object=quality,
                                                                                 pool_id=self.pool_id)

                quality['id'] = remote_quality['id']
                yield 'quality', quality_idx + 1, len(metadata.qualities)

            quality_lookup_instance = quality_lookups_api.QualityLookupsApi(client)
            quality_lookups = metadata.get_lookups('qualities')
            for lookup_idx, (key, idx) in enumerate(quality_lookups):
                lookup = LookupObject(id=metadata.qualities[idx]['id'], key=key, pool_id=self.pool_id)

                quality_lookup_instance.api_quality_lookups_pool_id_post(lookup_object=lookup, pool_id=self.pool_id)
                yield 'quality lookups', lookup_idx + 1, len(quality_lookups)
            # endregion

    def run(self, imports: List[ModelImport],
            dry_run: bool = False, skip_exist_sample: bool = False) -> ImportRunResult:
        results = self.check(imports)

        # Skip existing samples
        if skip_exist_sample:
            skip_idxs = set()
            for message in results['messages']:
                # Only process sample remote duplicate messages
                if 'SiteCode' not in message['fields'] \
                        or message['stage'] != ImportCheckStage(3) or message['status'] != ImportCheckStatus(0):
                    continue
                skip_idxs.add(message['index'])

            imports = [r for idx, r in enumerate(imports) if idx not in skip_idxs]
            results = self.check(imports)

        # Unhandled error messages
        if results['has_error_message']:
            raise Exception(f'Unhandled import check errors: {results["messages"]}')

        # Return nothing for dry run
        if dry_run:
            return ImportRunResult(imported_sample_count=0, imported_variant_count=0, imported_value_count=0)

        # Perform import
        with splashback_data.ApiClient(self._configuration) as client:
            instance = imports_api.ImportsApi(client)

            return instance.api_imports_run_pool_id_post(model_import=imports, pool_id=self.pool_id)
