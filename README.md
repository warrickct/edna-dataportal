# Bioplatforms Australia - Operational taxonomic unit (OTU) query system

BPA-OTU is a web-based portal into Operational Taxonomic Unit (OTU) data, developed to access data from the Australian Microbiome.

## Quick Setup

* [Install docker and compose](https://docs.docker.com/compose/install/)
* git clone [https://github.com/muccg/bpaotu.git](https://github.com/muccg/bpaotu.git)
* `./develop.sh build base`
* `./develop.sh build builder`
* `./develop.sh build dev`

`develop.sh up` will spin up the stack. See `./develop.sh` for some utility methods, which typically are simple
wrappers arround docker and docker-compose.

## Input data

BPA-OTU loads input data to generate a PostgreSQL schema named `otu`. The importer functionality completely
erases all previously loaded data.

Three categories of file are ingested:

* contextual metadata (XLSX format; data import is provided for Marine Microbes and BASE metadata)
* taxonomy files (extension: `.tax`)
* OTU abundance tables (extension: `.otu`)

All files should be placed under a base directory, and then the ingest can be run as a Django management command:

## Running ingestion

SSH into the host running this server application.

Navigate to the root directory where the application is located. For Auckland's instance of the application this will be
```
cd ~/repos/edna-bpaotu
```

```bash
docker exec -it bpaotu_runserver_1 bash
$ docker-compose exec runserver bash

# This commented code was the previous way and now deprecated.
# root@420c1d1e9fe4:~ /app/docker-entrypoint.sh django-admin otu_ingest /data/
```

### Adding password protected data

There is support for primitive password protected datasets. To do this, add a column with the header "password" and in enter in the password as plain text in the cells which require a password.

If there is no password column is provided, the password will be null and the data will be returned in public results.

## Accessing the database
```
docker exec -it bpaotu_db_1 bash
psql -U webapp
set search_path=otu;
```



### Contextual Metadata

These files are managed by Bioplatforms Australia. The latest version of these files can be found at the
[Bioplatforms Australia data portal](https://data.bioplatforms.com).

### Taxonomy files

A tab-delimited file with extension '.tax'

The first row of this file is a header, and has the form:

```tsv
OTU ID\tkingdom\tphylum\tclass\torder\tfamily\tgenus
```

Each column has the following format:

* OTU ID: a string describing the OTU (GATC string)
* technology: text string (e.g. 16S, A16S, 18S, ITS, ...)
* kingdom: text string
* phylum: text string
* class: text string
* order: text string
* family: text string
* genus: text string

### Abundance files

A tab-delimited file with the extension `.otu`

The first row is a header, with the following format:

* OTU ID: header for OTU ID column
* Sample ID [repeated]: the identifier for the sample ID for which this column specifies abundance

Each following has the following format:

* OTU ID: the OTU ID (text string, corresponding to the strings in the taxonomy file)
* Abundance [repeated]: the abundance (floating point) for the column's sample ID

## Development

Ensure a late version of both docker and docker-compose are available in your environment.

bpaotu is available as a fully contained Dockerized stack. The dockerised stack are used for both production
and development. Appropiate configuration files are available depending on usage.

Note that for data ingestion to work you need passwords to the hosted data, these are available from BPA on request.
Set passwords in your environment, these will be passed to the container.

## Deployments

[Bioplatforms Australia - Australian Microbiome Search Facility](https://data.bioplatforms.com/bpa/otu/)

## Licence

Copyright &copy; 2017, Bioplatforms Australia.

BPA OTU is released under the GNU Affero GPL. See source for a licence copy.

## Contributing

* Fork next_release branch
* Make changes on a feature branch
* Submit pull request