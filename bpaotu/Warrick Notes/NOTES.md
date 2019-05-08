
# Grahame's Notes

## Set up editor

cd ~/bpaotu
. ~/venv/bin/activate
code .

## Start app

Grahame's instruction: docker-compose up
(Make sure you're in the ~/bpaotu directory)

## Copy in the data

cp -r /path/to/2018-03 ./data/dev/

## Extract whichever amplicon

cd data/dev/2018-03
tar xzvf 16S.tar.gz

## Run the ingest

docker-compose exec runserver bash
/app/docker-entrypoint.sh django-admin otu_ingest /data/2018-03/

Note: If on nectar vm there is no need to include 2018-03/ directory
/app/docker-entrypoint.sh django-admin otu_ingest /data/

## Getting into the DB

warrick@warrick-OptiPlex-9030-AIO:~/bpaotu$ docker-compose exec db bash
root@d24533722fbf:/# psql -U postgres webapp
List the Django tables:
webapp=# set search_path=public;
webapp=# \dt
Get into the OTU schema (SQLAlchemy rather than Django)
webapp=# set search_path=otu;
webapp=# \dt

## TODO

Edit otu.py columns to accommodate our data
Edit query.py (gradual filtering etc) to accommodate our data
Create autofills for known categories from our data.

### Sub-todo

* Find out if importer.py or otu_ingest.py need to be altered for when our data is imported. Examine how their settings are determined.

## Django files

otu.py - responsible for sorting the ingested data into columns for the database schema.
query.py - uses SQLAlchemy to generate iterative queries. Useful for joining queries together.

## Docker things

/app/docker-entrypoint.sh django-admin otu_ingest /data/2018-03/ basically clears out the adundance and contextual databases and replaces it with a new one made from the importer + the data specified.

## Website for testing

<http://localhost:8000/> (should be at least). Also there's a /log and /tables subdirectory that show some dev things.

## Details

### Ingesting the data

1. not 100% sure what docker-cmpose exec runserver does. Runserver is within one of the docker compose files though.

2. call docker-entrypoint.sh then django-admin with the command otu_ingest. otu_ingest can be found in ~/bpaotu/bpaotu/management/commands/otu_ingest.py

3. Uses basecommand and DataImporter

4. Uses the DataImporter class to make an importer.

5. Calls importer functions. One function creates a lookup table

When I ran the ingest command using just the /data directory as the path (therefore including the gavin water data files) I got the error index not found from function load_soil_metadata in importer.py so I'm assuming I will actually need to alter something here.

### The way the ingester works (when running the ingester script)

1. loads in the marine and soil contextual data.

2. First pass on the .taxonomy file it defines ontologies

3. Second pass on .taxonomy file it defines OTUs

4. Writes a new file temporarily stores taxonomy data for processing which tuples looks like this: 215520,AA ... GTGC,2,65,197,466,848,2754,1,1

5. Reads through the .taxonomy file once again and references the temp taxonomy data file on step 4.

6. Now goes though OTU abundance tables (/data/2018-03/16S/AMD_16S_table_20180223.txt). Does two passes for some reason.

7. Writes OTU abundance data to new csv tempfile /data/bpaotu-6ig_m8up

8. Loads in the data from the step 7 temp file into the database I think?

### steps for updating production database and application.

1. update frontend the api url from dev to prod and run npm build so it updates the main.js file.

2. make a saved version of the current frontend master branch and push it to the remote.

3. push the compatible version of frontend to the master branch

4. push the data files to the edna-data repo

5. push the updated dataportal version to the master (and make a new branch of the  master pre-push)

6. ssh into the server ssh -i pathtoprivatekey ubuntu@ipOfeDNA

7. pull the new data portal branch

8. pull the new edna-data repo and replace the existing one in the dataportal project.

9. in views.py replace disable use_cors otherwise responses contain double asterisks in the cors field.
