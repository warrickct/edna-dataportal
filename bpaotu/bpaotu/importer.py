import os
import csv
import tempfile
import traceback
import logging
import sqlalchemy
from sqlalchemy.schema import CreateSchema, DropSchema
from sqlalchemy.sql.expression import text
from hashlib import md5
from sqlalchemy.orm import sessionmaker
from glob import glob
from .contextual import (
    marine_contextual_rows,
    soil_contextual_rows,
    soil_field_spec,
    marine_field_specs)
from collections import (
    defaultdict,
    OrderedDict)
from itertools import zip_longest
from .models import (
    ImportSamplesMissingMetadataLog,
    ImportFileLog,
    ImportOntologyLog)
from .otu import (
    Base,
    Environment,
    OTUKingdom,
    OTUPhylum,
    OTUClass,
    OTUOrder,
    OTUFamily,
    OTUGenus,
    OTUSpecies,

    # w: Including OTU to write directly to it.
    OTU,

    # sample_contextuals
    SampleContext,

    # edna phase 3
    SampleEnvironmentalMaterial1,
    SampleEnvironmentalMaterial2,

    SCHEMA,
    make_engine)

# w: for clearing sample_otu cache upon import.
from django.core.cache import caches
from hashlib import sha256
import re

# post import calculations
from .query import(
    EdnaPostImport
)

logger = logging.getLogger("rainbow")


def try_int(s):
    try:
        return int(s)
    except ValueError:
        return None


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def otu_hash(code):
    return md5(code.encode('ascii')).digest()

def site_hash(code):
    return md5(code.encode('ascii')).digest()

class DataImporter:
    # marine_ontologies = OrderedDict([
    #     ('environment', Environment),
    #     ('sample_type', SampleType),
    # ])
    edna_sample_ontologies = OrderedDict([
        ('sample_environmental_feature1', SampleEnvironmentalMaterial1),
        ('sample_environmental_feature2', SampleEnvironmentalMaterial2)
    ])

    def __init__(self, import_base):
        self._clear_import_log()
        self._engine = make_engine()
        Session = sessionmaker(bind=self._engine)
        self._create_extensions()
        self._session = Session()
        self._import_base = import_base
        try:
            self._session.execute(DropSchema(SCHEMA, cascade=True))
        except sqlalchemy.exc.ProgrammingError:
            self._session.invalidate()
        self._session.execute(CreateSchema(SCHEMA))
        self._session.commit()
        Base.metadata.create_all(self._engine)

    def _clear_import_log(self):
        logger.critical("Clearing import log")
        for log_cls in (ImportSamplesMissingMetadataLog, ImportFileLog, ImportOntologyLog):
            log_cls.objects.all().delete()

    def _create_extensions(self):
        extensions = ('citext',)
        for extension in extensions:
            try:
                logger.info("creating extension: %s" % extension)
                self._engine.execute('CREATE EXTENSION %s;' % extension)
            except sqlalchemy.exc.ProgrammingError as e:
                if 'already exists' not in str(e):
                    logger.critical("couldn't create extension: %s (%s)" % (extension, e))

    def _read_tab_file(self, fname):
        with open(fname) as fd:
            reader = csv.DictReader(fd, dialect="excel-tab")
            yield from reader

    def _build_ontology(self, db_class, vals):
        for val in sorted(vals):
            instance = db_class(value=val)
            self._session.add(instance)
        self._session.commit()
        return dict((t.value, t.id) for t in self._session.query(db_class).all())

    def _load_ontology(self, ontology_defn, row_iter):
        '''
        import the ontologies, and build a mapping from
        permitted values into IDs in those ontologies
        '''
        by_class = defaultdict(list)
        for field, db_class in ontology_defn.items():
            by_class[db_class].append(field)

        # each unique category for an a classification level.
        # w: goes through the list of categories under an item
        # w: if the row contains one of them add the value to the set.
        vals = defaultdict(set)
        for row in row_iter:
            for db_class, fields in by_class.items():
                # logger.info(fields)
                for field in fields:
                    if field in row:
                        vals[db_class].add(row[field])

        mappings = {}
        for db_class, fields in by_class.items():
            map_dict = self._build_ontology(db_class, vals[db_class])
            for field in fields:
                mappings[field] = map_dict
        return mappings

    @classmethod
    def classify_fields(cls, environment_lookup):
        # flip around to name -> id
        pl = dict((t[1], t[0]) for t in environment_lookup.items())
        soil_fields = set()
        marine_fields = set()
        for field_info in soil_field_spec:
            field_name = field_info[0]
            if field_name in DataImporter.soil_ontologies:
                field_name += '_id'
            soil_fields.add(field_name)
        for data_type, fields in marine_field_specs.items():
            for field_info in fields:
                field_name = field_info[0]
                if field_name in DataImporter.marine_ontologies:
                    field_name += '_id'
                marine_fields.add(field_name)
        soil_only = soil_fields - marine_fields
        marine_only = marine_fields - soil_fields
        r = {}
        r.update((t, pl['Soil']) for t in soil_only)
        r.update((t, pl['Marine']) for t in marine_only)
        return r

    def load_edna_taxonomies(self):

        otu_lookup = {}
        ontologies = OrderedDict([
            ('kingdom', OTUKingdom),
            ('phylum', OTUPhylum),
            ('class', OTUClass),
            ('order', OTUOrder),
            ('family', OTUFamily),
            ('genus', OTUGenus),
            ('species', OTUSpecies),
        ])

        def _normalize_taxonomy(ontology_parts):
            '''
            Pads or trims the taxonomic list size to match the number of columns in the otu table.
            '''
            changes = 0
            # TEMP: Stripping the prefix from ontology segments.
            for index, part in enumerate(ontology_parts):
                ontology_parts[index] = re.sub('[A-z]__', '', part)
            while len(ontology_parts) < len(ontologies):
                unclassified_padding = ''
                ontology_parts.append(unclassified_padding)
                changes += 1
            while len(ontology_parts) > len(ontologies):
                ontology_parts = ontology_parts[:-1]
                changes -= 1
            assert(len(ontology_parts) == len(ontologies))
            return ontology_parts

        def _taxon_rows_iter():
            '''
            Iterates over abundance file. Returns segmented version of the name otu's name field using ';' as the delimiting character.
            '''
            for fname in sorted(glob(self._import_base + 'edna/separated-data/data/*.tsv')):
                # logger.info("Reading taxonomy file: %s" % fname)
                with open(fname) as file:
                    reader = csv.DictReader(file, delimiter='\t')
                    imported = 0
                    for index, row in enumerate(reader):
                        # w: Uses the name as the lookup to find the PK of the taxon.
                        otu_lookup[otu_hash(row[''])] = index
                        otu = row['']
                        ontology_parts = otu.split(';')
                        ontology_parts = _normalize_taxonomy(ontology_parts)
                        obj = dict(zip(ontologies.keys(), ontology_parts))
                        obj['otu'] = otu
                        imported += 1
                        yield obj
                ImportFileLog.make_file_log(fname, file_type='Taxonomy', rows_imported=imported, rows_skipped=0)

        logger.warning("Loading eDNA taxonomies - pass 1, defining OTU ontologies")
        mappings = self._load_ontology(ontologies, _taxon_rows_iter())

        logger.info("loading eDNA taxonomies - pass 2, defining OTUs")
        try:
            with tempfile.NamedTemporaryFile(mode='w', dir='/data', prefix='bpaotu-', delete=False) as temp_fd:
                fname = temp_fd.name
                os.chmod(fname, 0o644)
                logger.warning("writing out OTU data to CSV tempfile: %s" % fname)
                w = csv.writer(temp_fd)
                w.writerow(['id', 'code', 'kingdom_id', 'phylum_id', 'class_id', 'order_id', 'family_id', 'genus_id', 'species_id', 'endemic'])
                for _id, row in enumerate(_taxon_rows_iter(), 1):
                    # create lookup entry
                    otu_lookup[otu_hash(row['otu'])] = _id
                    out_row = [_id, row['otu']]
                    for field in ontologies:
                        if field not in row:
                            out_row.append('')
                        else:
                            out_row.append(mappings[field][row[field]])
                    out_row.append("False")
                    w.writerow(out_row)
            logger.warning("loading taxonomy data from temporary CSV file")
            self._engine.execute(
                text('''COPY otu.otu from :csv CSV header''').execution_options(autocommit=True),
                csv=fname)
        finally:
        #     os.unlink(fname)
            return otu_lookup

    def load_edna_contextual_metadata(self):
        '''
        - Had to clean the fields with regex expressions that match the ones used to clean the sample_context columns in otu.py
        - Also made a sitelookup to pass in as our site data doesn't contain site data.
        '''

        def _clean_value(value):
            ''' Makes sure the value for the entry is uniform '''
            if isinstance(value, str):
                value.upper()
            return value

        def _clean_field(field):
            ''' Makes sure the field matches the database column name '''
            replacements = [
                ('\s', '_'),
                ('&', '_and_'),
                ('/', '_or_'),
                ('-', '_dash_'),
                ('\(|\)', '_bracket_'),
                ('_{2,}', '_'),
            ]
            for old, new in replacements:
                field = re.sub(old, new, field)
            field = field.lower()
            # Made all the fields have a underscore at the start to prevent python word conflicts. Probably need a better solution.
            # logger.info(field)
            return field

        def _make_context():
            '''
            Iterates the metadata, Makes an object mirror a sample_context tuple and returns it 
            TODO: Allow for automated 0 values when a field is missing.
            '''

            logger.info('loading edna contextual metadata from .tsv files')
            # site_id delcared here so we can go over multiple files at once.
            site_id = 0
            for fname in sorted(glob(self._import_base + 'edna/separated-data/metadata/*mastersheet.tsv')):
                # logger.warning('loading metadata file: %s' % fname)
                with open(fname, "r", encoding='utf-8-sig') as file:
                    reader = csv.DictReader(file, delimiter='\t')
                    for file_row in reader:
                        attrs ={}
                        # DEBUG: 
                        logger.info(fname)
                        # logger.info(site_lookup)
                        try:
                            site_lookup[site_hash(file_row['site'].upper())] = site_id
                        except:
                            # exception for new metadata structure
                            site_lookup[site_hash(file_row['Sample_identifier'].upper())] = site_id
                        # testing it won't grab two site id entries instead of overwrite existing
                        # logger.info(site_hash(row['Sample_identifier'].upper()))

                        # DEBUG:
                        attrs['id'] = site_id
                        for edna_ontology_item in DataImporter.edna_sample_ontologies:
                            # if it's an ontology field just add '_id' to the end of the name
                            if edna_ontology_item not in file_row:
                                continue
                            attrs[_clean_field(edna_ontology_item) + '_id'] = mappings[edna_ontology_item][file_row[edna_ontology_item]]
                        for edna_ontology_item, value in file_row.items():
                            cleaned_field = _clean_field(edna_ontology_item)
                            if cleaned_field in attrs or (cleaned_field + '_id') in attrs:
                                continue
                            attrs[cleaned_field] = _clean_value(value)
                            if _clean_value(value) == '' or _clean_value(value) == ' ':
                                attrs[cleaned_field] = 0
                        site_id += 1
                        logger.info(attrs)
                        yield SampleContext(**attrs)

        def _combined_rows():
            '''
            Custom to eDNA project - aggregate row iterable for iterating over multiple files
            '''
            for fname in sorted(glob(self._import_base + 'enda/separated-data/metadata/*.tsv')):
                with open(fname, "r") as file:
                    reader = csv.reader(file, delimiter='\t')
                    headers = next(reader)
                    for row in reader:
                        # adding compatibility for the ontology builder.
                        dict_row = {}
                        for index, field in enumerate(row):
                            dict_row[headers[index]] = field
                        yield dict_row

        # custom site lookup dictionary edna ones use the code rather than PK in the data files. For faster abundance loading
        site_lookup = {}
        mappings = self._load_ontology(DataImporter.edna_sample_ontologies, _combined_rows())
        self._session.bulk_save_objects(_make_context())
        self._session.commit()
        return site_lookup
        
    def load_edna_otu_abundance(self, otu_lookup, site_lookup):

        def _validate_count(count):
            try:
                return float(count)
            except:
                return 0

        def _validate_sample_id(column):
            try:
                if site_lookup[site_hash(column.upper())] is None:
                    # TODO: NEED TO FIX unharmonious Syrie site entries.
                    print("skipping site: " + site_lookup[site_hash(column.upper())])
                    # exit()
                return site_lookup[site_hash(column.upper())]
            except:
                print(column)

        # TODO: need to update data cleaners
        sample_otu_combinations_used = []
        def check_for_duplicates(sample_id, otu_id, count):
            # logger.info(sample_id)
            return None
            # logger.info(otu_id)
            # logger.info(count)

        def _make_sample_otus():    
            for fname in sorted(glob(self._import_base + 'edna/separated-data/data/*.tsv')):
                logger.info('writing abundance rows from %s' % fname)
                file = open(fname, 'r')
                reader = csv.DictReader(file, delimiter='\t')
                for row in reader:
                    otu_code = row['']
                    otu_id = otu_lookup[otu_hash(otu_code)]
                    for sample_context_col in row:
                        if sample_context_col == '':
                            continue
                        sample_id = _validate_sample_id(sample_context_col)
                        if sample_id is None:
                            continue
                        count = _validate_count(row[sample_context_col])
                        # check_for_duplicates(sample_id, otu_id, count)
                        # logger.info(10/0)
                        if count > 0:
                            # count is already proportional, can be copied into proportional count column
                            if count < 1:
                                yield [sample_id, otu_id, count, count]
                            else:
                                # add as a yet to be calculated field
                                # works because we assume organism presence in order to be in abundance table.
                                yield [sample_id, otu_id, count, 0]

        def _clear_edna_caches():
            # TODO: Get rid of magic string cache references
            # clearing sample_otu cache
            logger.info('deleting edna sample otu cache.')
            cache = caches['edna_sample_otu_results']
            hash_str = 'eDNA_Sample_OTUs:cached'
            key = sha256(hash_str.encode('utf8')).hexdigest()
            cache.delete(key)
            # clearing otu cache
            logger.info('Clearing edna taxonomy options cache.')
            cache = caches['edna_taxonomy_options_results']
            hash_str = 'eDNA_Taxonomy_Options:cached'
            key = sha256(hash_str.encode('utf8')).hexdigest()
            cache.delete(key)

        logger.warning("Starting edna abundance loader.")
        with tempfile.NamedTemporaryFile(mode='w', dir='/data', prefix='bpaotu-', delete=False) as temp_fd:
            fname = temp_fd.name
            os.chmod(fname, 0o644)
            logger.warning("writing out OTU abundance to csv tempfile: %s" % fname)
            w = csv.writer(temp_fd)
            w.writerow(['sample_id', 'otu_id', 'count', 'proportional_abundance'])
            w.writerows(_make_sample_otus())
        try:
            self._engine.execute(
                    text('''COPY otu.sample_otu from :csv CSV header''').execution_options(autocommit=True),
                    # text('''COPY otu.sample_otu(sample_id, otu_id, count) from :csv CSV header''').execution_options(autocommit=True),
                    csv=fname)
            _clear_edna_caches()
        except:
            logger.critical("unable to import")
            traceback.print_exc()
        with EdnaPostImport() as post_import:
            post_import._calculate_endemic_otus()
            post_import._normalize_abundances()
