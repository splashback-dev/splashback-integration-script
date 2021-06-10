# Splashback Integration Script (SIS)

Extensible script to integrate data sources with Splashback.

## Examples

### Importing from THREDDS in NetCDF format

The following example uses the AODN (Australian Observational Data Network) THREDDS Data Servver (TDS):

```shell
python main.py -v -d tmp --pool-id 2005 --dry-run \
               -f thredds --thredds-dataset 'IMOS/ANMN/NRS/NRS.*/Biogeochem_profiles/.*\.nc' --thredds-dataset-pattern --thredds-service httpService \
               -p netcdf --netcdf-mapping mappings/netcdf/imos_anmn_nrs_biogeochem.json
```

## Installation

### Dependencies

- netCDF4
- requests
- splashback_data (generated OpenAPI library, see the docs here)

:wrench: TODO :wrench:

## Structure

### 1. Finders

Finders are used to collect data and provide a list of file system paths to be parsed. They live in the `finder`
package.

### 2. Parsers

Parsers take a single path and parse that file into a list of imports. They live in the `parser` package.

### 3. Importer

The importer takes a list of imports and sends them to Splashback.

## Contributing

:wrench: TODO :wrench:

## Licence

:wrench: TODO :wrench:
