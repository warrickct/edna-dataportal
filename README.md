# eDNA Dataportal

This repository is a variation of the Bioplatforms Australia dataportal which has been adapted to be a backend for the eDNA Virtual Hub.

## Quick Setup

* [Install docker and compose](https://docs.docker.com/compose/install/)
* git clone [https://github.com/warrickct/edna-dataportal.git](https://github.com/warrickct/edna-dataportal.git)
* `./develop.sh build base`
* `./develop.sh build builder`
* `./develop.sh build dev`

`develop.sh up` will spin up the stack. See `./develop.sh` for some utility methods, which typically are simple
wrappers arround docker and docker-compose.

## Input data

The eDNA dataportal loads input data to generate a PostgreSQL schema named `otu`. The importer functionality completely
erases all previously loaded data.

Three categories of file are ingested:

* contextual metadata (XLSX format; data import is provided for Marine Microbes and BASE metadata)
* OTU abundance tables (extension: `.tsv`)

All files should be placed under a base directory, and then the ingest can be run as a Django management command:

```bash
$ docker-compose exec runserver bash
root@420c1d1e9fe4:~# /app/docker-entrypoint.sh django-admin otu_ingest /data/otu/
```

### Contextual Metadata

This is an `.tsv` file that contains the name of sites where eDNA was sampled from and contains metrics regarding the sample.

### Abundance files

A tab-delimited file with the extension `.tsv`

The first row is a header, with the following format:

* OTU code: The OTU name column.
* Sample ID [repeated]: the identifier for the sample ID for which this column specifies abundance

Each following has the following format:

* OTU code: The full taxonomic name of the organism delimited with "`;`" (text string, corresponding to the strings in the taxonomy file)
* Abundance [repeated]: the abundance (floating point) for the column's sample ID

## Development

Ensure a late version of both docker and docker-compose are available in your environment.

bpaotu is available as a fully contained Dockerized stack. The dockerised stack are used for both production
and development. Appropiate configuration files are available depending on usage.

Note that for data ingestion to work you need passwords to the hosted data, these are available from BPA on request.
Set passwords in your environment, these will be passed to the container.

## Licence

Copyright &copy; 2017, Bioplatforms Australia.

BPA OTU is released under the GNU Affero GPL. See source for a licence copy.

## Contributing

* Fork next_release branch
* Make changes on a feature branch
* Submit pull request