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

## Contributing

:wrench: TODO :wrench:

## Licence

:wrench: TODO :wrench:
