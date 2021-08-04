# Splashback Integration Script (SIS)

Extensible script to integrate data sources with Splashback.

## Examples

For help, run the following command:

```shell
$ python main.py -h
```

### Importing from THREDDS in NetCDF format

The following example uses the AODN (Australian Observational Data Network) THREDDS Data Server (TDS):

```shell
$ python main.py -v -d tmp --pool-id 2005 --dry-run \
                 -f thredds --thredds-dataset 'IMOS/ANMN/NRS/NRS.*/Biogeochem_profiles/.*\.nc' --thredds-dataset-pattern --thredds-service httpService \
                 -p netcdf --netcdf-mapping mappings/netcdf/imos_anmn_nrs_biogeochem.json
```

## Installation

We recommend you setup a `venv` for SIS development. This gives you an isolated environment separate from your system.

```shell
$ python3 -m venv venv
$ source venv/bin/activate
(venv) $ # Congrats, you're now in the new venv!
```

### Dependencies

1. Install requirements.

```shell
(venv) $ pip install -r requirements.txt
```

2. [Generate the Splashback Data OpenAPI library.](https://docs.splashback.io/reference/api-docs/rest#generating-client-libraries)
3. Install the generated library.

```shell
(venv) $ pip install <path_to_generated_library>
```

### Authentication

Authenticating with Splashback is easy. Simply set the `SPLASHBACK_API_KEY` environment variable in your session.
Alternatively, create a new file, `.env`, with the following content:

```shell
SPLASHBACK_API_KEY=<your_api_key>
```

For more information on API keys, see the docs [here](https://docs.splashback.io/reference/api-docs/api-keys).

## Structure

### 1. Finders

Finders are used to collect data and provide a list of file system paths to be parsed. They live in the `finder`
package.

They need to be registered in `main.py`, along with any arguments they require.

### 2. Parsers

Parsers take a single path and parse that file into a list of imports. They live in the `parser` package.

They need to be registered in `main.py`, along with any arguments they require.

### 3. Importer

The importer takes a list of imports and sends them to Splashback. This component should not need to be extended by you.

## Contributing

:wrench: TODO :wrench:

## Licence

:wrench: TODO :wrench:
