import datetime
from functools import partial
from hashlib import sha256
from itertools import chain
import logging

import time
import csv

import sqlalchemy
from sqlalchemy import (
    and_,
    or_,
    func,
    update,
)
from sqlalchemy.orm import (
    sessionmaker,
)

from django.core.cache import caches

from .otu import (
    OTU,
    OTUKingdom,
    OTUPhylum,
    OTUClass,
    OTUOrder,
    OTUFamily,
    OTUGenus,
    OTUSpecies,
    SampleContext,
    SampleOTU,
    SampleType,
    make_engine)


logger = logging.getLogger("rainbow")
engine = make_engine()
Session = sessionmaker(bind=engine)


class OTUQueryParams:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class TaxonomyOptions:
    hierarchy = [
        ('kingdom_id', OTUKingdom),
        ('phylum_id', OTUPhylum),
        ('class_id', OTUClass),
        ('order_id', OTUOrder),
        ('family_id', OTUFamily),
        ('genus_id', OTUGenus),
        ('species_id', OTUSpecies),
    ]

    def __init__(self):
        self._session = Session()

    def __enter__(self):
        return self

    def __exit__(self, exec_type, exc_value, traceback):
        self._session.close()

    def possibilities(self, amplicon, state):
        cache = caches['search_results']
        hash_str = 'TaxonomyOptions:cached:' + repr(amplicon) + ':' + repr(state)
        key = sha256(hash_str.encode('utf8')).hexdigest()
        result = cache.get(key)
        if not result:
            result = self._possibilities(amplicon, state)
            # w: removed cache to see filter results debugging
            # cache.set(key, result)
        return result

    def _possibilities(self, amplicon, state):
        """
        state should be a list of integer IDs for the relevent model, in the order of
        TaxonomyOptions.hierarchy. a value of None indicates there is no selection.
        """

        def drop_id(attr):
            "return without `_id`"
            return attr[:-3]

        def determine_target():
            # this query is built up over time, and validates the hierarchy provided to us
            q = self._session.query(OTU.kingdom_id).group_by(OTU.kingdom_id)
            q = apply_amplicon_filter(q, amplicon)
            for idx, ((otu_attr, ontology_class), taxonomy) in enumerate(zip(TaxonomyOptions.hierarchy, state)):
                valid = True
                if taxonomy is None or taxonomy.get('value') is None:
                    valid = False
                else:
                    q = apply_otu_filter(otu_attr, q, taxonomy)
                    valid = q.count() > 0
                if not valid:
                    return otu_attr, ontology_class, idx
            return None, None, None

        # scan through in order and find our target, by finding the first invalid selection
        target_attr, target_class, target_idx = determine_target()
        # the targets to be reset as a result of this choice
        clear = [drop_id(attr) for attr, _ in TaxonomyOptions.hierarchy[target_idx:]]

        # no completion: we have a complete hierarchy
        if target_attr is None:
            return {}
        # performance: hard-code kingdom (it's the slowest query, and the most common)
        elif not amplicon and target_class is OTUKingdom:
            possibilities = self._session.query(target_class.id, target_class.value).all()
        else:
            # clear invalidated part of the state
            state = state[:target_idx] + [None] * (len(TaxonomyOptions.hierarchy) - target_idx)
            # build up a query of the OTUs for our target attribute
            q = self._session.query(getattr(OTU, target_attr), target_class.value).group_by(getattr(OTU, target_attr), target_class.value).order_by(target_class.value)

            q = apply_amplicon_filter(q, amplicon)
            for (otu_attr, ontology_class), taxonomy in zip(TaxonomyOptions.hierarchy, state):
                q = apply_otu_filter(otu_attr, q, taxonomy)
            q = q.join(target_class)
            possibilities = q.all()

        result = {
            'new_options': {
                'target': drop_id(target_attr),
                'possibilities': possibilities,
            },
            'clear': clear
        }
        return result


class OntologyInfo:
    def __init__(self):
        self._session = Session()

    def __enter__(self):
        return self

    def __exit__(self, exec_type, exc_value, traceback):
        self._session.close()

    def get_values(self, ontology_class):
        vals = self._session.query(ontology_class.id, ontology_class.value).all()
        vals.sort(key=lambda v: v[1])
        return vals

# w: This is the one that queries for the abundances.
class SampleQuery:
    """
    find samples IDs which match the given taxonomical and
    contextual filters
    """

    def __init__(self, params):
        self._session = Session()
        # amplicon filter is a master filter over the taxonomy; it's not
        # a strict part of the hierarchy, but affects taxonomy options
        # available
        self._amplicon_filter = params.amplicon_filter
        self._taxonomy_filter = params.taxonomy_filter
        self._contextual_filter = params.contextual_filter

    def __enter__(self):
        return self

    def __exit__(self, exec_type, exc_value, traceback):
        self._session.close()

    def _q_all_cached(self, topic, q, mutate_result=None):
        cache = caches['search_results']
        hash_str = 'SampleQuery:cached:%s:' % (topic) \
            + repr(self._amplicon_filter) + ':' \
            + repr(self._taxonomy_filter) + ':' \
            + repr(self._contextual_filter)
        key = sha256(hash_str.encode('utf8')).hexdigest()
        result = cache.get(key)
        if not result:
            result = q.all()
            if mutate_result:
                result = mutate_result(result)
                # w: Warrick: comment out cache for testing
            # cache.set(key, result)
        return result

    # TODO: This doesn't work with the waterdata sample context table
    def matching_sample_ids_and_environment(self):
        q = self._session.query(SampleContext.id, SampleContext._x)
        subq = self._build_taxonomy_subquery()
        q = self._apply_filters(q, subq).order_by(SampleContext.id)
        return self._q_all_cached('matching_sample_ids_and_environment', q)

    def matching_sample_headers(self, required_headers=None, sort_col=None, sort_order=None):
        query_headers = [SampleContext.id, SampleContext.environment_id]
        joins = []  # Keep track of any foreign ontology classes which may be needed to be joined to.

        cache_name = ['matching_sample_headers']
        if required_headers:
            cache_name += required_headers
            for h in required_headers:
                if not h:
                    continue

                col = getattr(SampleContext, h)

                if hasattr(col, "ontology_class"):
                    foreign_col = getattr(col.ontology_class, 'value')
                    query_headers.append(foreign_col)
                    joins.append(col.ontology_class)
                else:
                    query_headers.append(col)

        q = self._session.query(*query_headers).outerjoin(*joins)

        if sort_order == 'asc':
            q = q.order_by(query_headers[int(sort_col)])

            cache_name.append(str(query_headers[int(sort_col)]))
            cache_name.append(sort_order)

        elif sort_order == 'desc':
            q = q.order_by(query_headers[int(sort_col)].desc())

            cache_name.append(str(query_headers[int(sort_col)]))
            cache_name.append(sort_order)

        return self._q_all_cached(':'.join(cache_name), q)

    def matching_samples(self):
        q = self._session.query(SampleContext)
        subq = self._build_taxonomy_subquery()
        q = self._apply_filters(q, subq).order_by(SampleContext.id)
        return self._q_all_cached('matching_samples', q)

    def has_matching_sample_otus(self, kingdom_id):
        def to_boolean(result):
            return result[0][0]

        q = self._session.query(self.matching_sample_otus(kingdom_id).exists())
        return self._q_all_cached('has_matching_sample_otus:%s' % (kingdom_id), q, to_boolean)

    def matching_sample_otus(self, kingdom_id):
        # we do a cross-join, but convert to an inner-join with
        # filters. as SampleContext is in the main query, the
        # machinery for filtering above will just work
        q = self._session.query(OTU, SampleOTU, SampleContext) \
            .filter(OTU.id == SampleOTU.otu_id) \
            .filter(SampleContext.id == SampleOTU.sample_id)
        q = self._apply_taxonomy_filters(q)
        q = self._contextual_filter.apply(q)
        if kingdom_id is not None:
            q = q.filter(OTU.kingdom_id == kingdom_id)
        # we don't cache this query: the result size is enormous,
        # and we're unlikely to have the same query run twice.
        # instead, we return the sqlalchemy query object so that
        # it can be iterated over
        return q

    def _apply_taxonomy_filters(self, q):
        q = apply_amplicon_filter(q, self._amplicon_filter)
        for (otu_attr, ontology_class), taxonomy in zip(TaxonomyOptions.hierarchy, self._taxonomy_filter):
            q = apply_otu_filter(otu_attr, q, taxonomy)
        return q

    def _build_taxonomy_subquery(self):
        """
        return the BPA IDs (as ints) which have a non-zero OTU count for OTUs
        matching the taxonomy filter
        """
        # shortcut: if we don't have any filters, don't produce a subquery
        if not self._amplicon_filter and self._taxonomy_filter[0] is None:
            return None
        q = self._session.query(SampleOTU.sample_id).distinct().join(OTU)
        return self._apply_taxonomy_filters(q)

    def _apply_filters(self, sample_query, taxonomy_subquery):
        """
        run a contextual query, returning the BPA IDs which match.
        applies the passed taxonomy_subquery to apply taxonomy filters.

        paging support: applies limit and offset, and returns (count, [bpa_id, ...])
        """
        # we use a window function here, to get count() over the whole query without having to
        # run it twice
        q = sample_query
        if taxonomy_subquery is not None:
            q = q.filter(SampleContext.id.in_(taxonomy_subquery))
        # apply contextual filter terms
        q = self._contextual_filter.apply(q)
        return q

# w: TEST: Making a test query to mimic the .tsv data for now.
class EdnaAbundanceQuery:
    def __init__(self):
        self._session = Session()

    def __enter__(self):
        return self

    def __exit__(self, exec_type, exc_value, traceback):
        self._session.close()

    def get_abundance_nested(self, term):
        results = []
        otu_lookup = dict(self._session.query(OTU.id, OTU.code).all())
        site_lookup = dict(
            self._session.query(SampleContext.id, SampleContext._site).all()
        )
        if not term:
            abundance_rows = (
                self._session.query(SampleOTU.count, SampleContext._site, OTU.code)
                .join(SampleContext)
                .join(OTU)
                # .all()
            )
        else:
            abundance_rows = (
                self._session.query(SampleOTU.count, SampleContext._site, OTU.code)
                .join(SampleContext)
                .join(OTU)
                .filter(OTU.code.like("%" + term + "%"))
                .all()
            )
        abundance_nested = {}
        for abundance_entry in abundance_rows:
            count = abundance_entry[0]
            site = abundance_entry[1]
            otu = abundance_entry[2]
            if otu not in abundance_nested:
                abundance_nested[otu] = {}
            if site not in abundance_nested[otu]:
                abundance_nested[otu][site] = count
        for otu_key in abundance_nested:
            row = {
                "": otu_key
            }
            for site_key in abundance_nested[otu_key]:
                row[site_key] = abundance_nested[otu_key][site_key]
            results.append(row)
        return results


class EdnaMetadataQuery:
    def __init__(self):
        self._session = Session()

    def __enter__(self):
        return self

    def __exit__(self, exec_type, exc_value, traceback):
        self._session.close()

    def get_all_metadata(self, ids=None):
        if ids:
            query = (
                self._session.query(
                    SampleContext.id,
                    SampleContext._site,
                    SampleContext._x,
                    SampleContext._y,
                    SampleContext._elev,
                    SampleContext._mean_c_percent,
                    SampleContext._mid_ph,
                    SampleContext._ave_lognconcen,
                    SampleContext._prec_mean,
                    SampleContext._water2,
                    SampleContext._freshwater
                    )
                .filter(SampleContext.id.in_(ids))
                .order_by(SampleContext.id)
                .all()
            )
        else:
            query = (
                self._session.query(
                    SampleContext.id,
                    SampleContext._site,
                    SampleContext._x,
                    SampleContext._y,
                    SampleContext._elev,
                    SampleContext._mean_c_percent,
                    SampleContext._mid_ph,
                    SampleContext._ave_lognconcen,
                    SampleContext._prec_mean,
                    SampleContext._water2,
                    SampleContext._freshwater,
                    )
                .order_by(SampleContext.id)
                .all()
            )
        # TODO: hardcoding dictionary response for now. Need to replace with automated keys based off table columns.
        results =[]
        for tuple in query:
            results.append({
                'id': tuple[0],
                'site': tuple[1],
                'x': tuple[2],
                'y': tuple[3],
                'elev': tuple[4],
                'mean_C_percent': tuple[5],
                'mid_ph': tuple[6],
                'ave_logNconcen': tuple[7],
                'prec_mean': tuple[8],
                'water2': tuple[9],
                'freshwater': tuple[10]
            })
        return results


# depcrecated:
class EdnaOrderedSampleOTU:
    def __init__(self):
        self._session = Session()

    def __enter__(self):
        return self

    def __exit__(self, exec_type, exc_value, traceback):
        self._session.close()

    def get_sample_otu_ordered(self):
        cache = caches['edna_sample_otu_results']
        hash_str = 'eDNA_Sample_OTUs:cached'
        key = sha256(hash_str.encode('utf8')).hexdigest()
        result = cache.get(key)
        if not result:
            logger.info("sample_otu_cache not found, making new cache")
            result = self._query_sample_otu_ordered()
            # cache.set(key, result)
        else:
            logger.info("Using cached sample_otu results")
        return result

    def _query_sample_otu_ordered(self):
        # assume otu.code and sample_context ordered by id
        otus = [r[0] for r in (
            self._session.query(OTU.code)
                .order_by(OTU.id)
                .all()
        )]
        sites = [r[0] for r in (
            self._session.query(SampleContext._site)
                .order_by(SampleContext.id)
                .all()
        )]
        abundances = (
                self._session.query(SampleOTU.otu_id, SampleOTU.sample_id, SampleOTU.count)
                .all()
        )
        response = {
            'otus': otus,
            'sites': sites,
            'abundances': abundances,
        }
        return response


class EdnaSampleContextualQuery:
    def __init__(self):
        self._session = Session()

    def __enter__(self):
        return self

    def __exit__(self, exec_type, exc_value, traceback):
        self._session.close()

    # some default caching for quicker results.
    def get_sample_contextual_options(self, filters):
        # TODO: Add caching (not super important for this one.)
        result = self.query_contextual_fields(filters)
        # cache = caches['edna_sample_contextual_fields']
        # hash_str = 'eDNA_Sample_OTUs:cached'
        # key = sha256(hash_str.encode('utf8')).hexdigest()
        # result = cache.get(key)
        # if not result:
        #     logger.info("sample_otu_cache not found, making new cache")
        #     result = self._query_sample_contextual_fields()
        #     #cache.set(key, result)
        # else:
        #     logger.info("Using cached sample_otu results")
        return result

    def query_contextual_fields(self, filters=None):
        '''
        Returns an list of all the columns in the sample_contextual fields used for suggestions
        '''
        field_results= [column.key for column in SampleContext.__table__.columns]
        return field_results

    def query_sample_contextuals(self, tags=None):
        '''
        Returns primary key set of sample_contextuals matching the filters
        '''
        def _row_to_dict(row):
            d = {}
            for column in row.__table__.columns:
                d[column.name] = (getattr(row, column.name))
            return d

        query = self._session.query(SampleContext)
        # iterative build the filter then join it all in one bang and filter at the end.
        sample_contextual_results = []
        if tags is not None:
            logger.info("contextual tags is not none.")
            or_filters = list()
            for tag in tags:
                # TODO: Add support for parsing AND and OR operators in the tags.
                # if filter contains a value to compare
                if '$' in tag:
                    filter_segments = tag.split('$')
                    field = filter_segments[0]
                    operation = filter_segments[1][:2]
                    value = filter_segments[1][2:]
                    if operation == "eq":
                        or_filters.append(getattr(SampleContext, field) == value)
                        # base_query = base_query.filter(getattr(SampleContext, field) == value)
                    if operation == "gt":
                        or_filters.append(getattr(SampleContext, field) > value)
                        # base_query = base_query.filter(getattr(SampleContext, field) > value)
                    if operation == "lt":
                        or_filters.append(getattr(SampleContext, field) < value)
                        # base_query = base_query.filter(getattr(SampleContext, field) < value)
            query = query.filter(or_(*or_filters))
        sample_contextual_results = [_row_to_dict(r) for r in query.all()]
        return sample_contextual_results


class EdnaOTUQuery:
    def __init__(self):
        self._session = Session()

    def __enter__(self):
        return self

    def __exit__(self, exec_type, exc_value, traceback):
        self._session.close()

    def _query_primary_keys(self, otus=None, use_endemism=False, endemic_value=False):
        '''
        Returns otu primary keys that match the search parameters. Currently used as part of the sample otu query for a filter.
        '''
        otu_columns = [OTU.kingdom_id, OTU.phylum_id, OTU.class_id, OTU.order_id, OTU.family_id, OTU.genus_id, OTU.species_id]
        otu_ids = []
        base_query = self._session.query(OTU.id)
        if otus is not None:
            for otu in otus:
                otu_query = base_query
                for index, ontological_id in enumerate(otu.split(' ')):
                    otu_column = otu_columns[index]
                    otu_query = otu_query.filter(otu_column == ontological_id)
                if use_endemism:
                    otu_query = otu_query.filter(OTU.endemic == endemic_value)
                otu_ids = otu_ids + [r[0] for r in otu_query]

        # if use_endemism:
        #     endemic_query = base_query.filter(OTU.endemic == endemic_value)
        #     otu_ids = otu_ids + [r[0] for r in endemic_query.all()]

        # removes duplicates
        otu_ids = list(set(otu_ids))
        logger.info(len(otu_ids))
        return otu_ids

    def get_taxonomy_options(self, filters, page=1, page_size=50):
        '''
        checks against list of codes with ids accompanying them. Returns the codes where the filter is contains within them.
        '''

        def _sort_by_element_index(elem):
            return elem[0]

        cache = caches['edna_taxonomy_options_results']
        hash_str = 'eDNA_Taxonomy_Options:cached'
        key = sha256(hash_str.encode('utf8')).hexdigest()
        result = cache.get(key)
        if not result:
            logger.info("Taxonomy option cache entry not found, making new cache")
            result = self._query_taxonomy_options()
            cache.set(key, result)
        else:
            logger.info("Using cached taxonomic options")
        # only only return results with the param(s)
        filters = filters.lower()
        result = [r for r in result if (filters in r[0].lower())]
        start = ((page -1) * page_size)
        end = ((page -1) * page_size) + page_size
        paginated_result = result[start:end]
        return {
            "result": paginated_result,
            "total_results": len(result)
        }

    def _query_taxonomy_options(self):
        '''
        Created the taxonomic search suggestions with their ontological keys are mapped.
        '''
        ontology_tables = [OTUKingdom, OTUPhylum, OTUClass, OTUOrder, OTUFamily, OTUGenus, OTUSpecies]
        ordered_otus = [r for r in (
            self._session.query(
                OTU.kingdom_id,
                OTU.phylum_id,
                OTU.class_id,
                OTU.order_id,
                OTU.family_id,
                OTU.genus_id,
                OTU.species_id,
                OTU.id
                )
            .order_by(
                OTU.kingdom_id,
                OTU.phylum_id,
                OTU.class_id,
                OTU.order_id,
                OTU.family_id,
                OTU.genus_id,
                OTU.species_id,
                )
            .all()
            )]
        # create lookup for performance
        otu_ontology_lookups = {}
        for table_index, table in enumerate(ontology_tables):
            if table_index not in otu_ontology_lookups:
                otu_ontology_lookups[table_index] = {}
            rows = [r for r in (self._session.query(table.id, table.value).all())]
            # going over ontological rows
            for tuple in rows:
                pk = tuple[0]
                text = tuple[1]
                otu_ontology_lookups[table_index][pk] = text
        # Reubild with the prefixes attached.
        prefixes = [
            "k__",
            "p__",
            "c__",
            "o__",
            "f__",
            "g__",
            "s__",
            "sid__"
            ]
        # generate the options with the pk field for faster searching.
        # possibly making it paginated.
        options = {}
        ordered_otus_iter = iter(ordered_otus)
        for otu in ordered_otus_iter:
            otu_text_segments = []
            combination_key = []
            for index, ontology_id in enumerate(otu[:len(otu) -1]):
                otu_segment = otu_ontology_lookups[index][ontology_id].strip(' ')
                taxon_prefix = prefixes[index]
                otu_pk = otu[len(otu) - 1]
                if otu_segment == '' or otu_segment == ' ':
                    next(ordered_otus_iter)
                otu_text_segments.append(taxon_prefix + otu_segment)
                combination_key.append(ontology_id)
                otu_key = ';'.join(otu_text_segments)
                if otu_key not in options:
                    options[otu_key] = [combination_key[:index + 1], otu_pk]
        # converting it back to list from for easy use on front end
        option_list = []
        for key, value in options.items():
            option_list.append([key, value[0], value[1]])
        return option_list

    def get_otu_names(self, primary_keys=None):
        # accepts a list of primary keys, returns the otu names/codes where possible.
        if (primary_keys is None):
            return None

        query = (self._session.query(OTU.id, OTU.code).filter(OTU.id.in_(primary_keys)).all())
        otu_codes = [r._asdict() for r in query]
        return otu_codes
    
    def get_otu_pathogenic_status_by_id(self, primary_keys = None):
        '''
        Returns input otus grouped into in pathogenic and nonpathogenic categories.
        '''
        if (primary_keys is None):
            return

        result = {}
        pathogenic = []
        # nonpathogenic = []
        for x in [r for r in self._session.query(OTU.id, OTU.pathogenic).filter(OTU.id.in_(primary_keys))]:
            if x[1] is True:
                pathogenic.append(x[0])
            # else:
            #     nonpathogenic.append(x[0])
        # result['pathogenic'] = pathogenic
        result = pathogenic
        # result['nonpathogenic'] = nonpathogenic
        return result

class EdnaSampleOTUQuery:
    def __init__(self):
        self._session = Session()

    def __enter__(self):
        return self

    def __exit__(self, exec_type, exc_value, traceback):

        self._session.close()

    def query_sample_otus(self, otu_ids=None, sample_contextual_ids=None, use_union=None):
        '''
        if user requesting all otus then send a cached version.
        '''
        if otu_ids is None and sample_contextual_ids is None and (use_union is None or use_union is True):
            logger.info("returning entire sample otu data")
            cache = caches['edna_sample_otu_results']
            hash_str = 'eDNA_Sample_OTUs:cached'
            key = sha256(hash_str.encode('utf8')).hexdigest()
            result = cache.get(key)

            if not result:
                logger.info("sample_otu_cache not found, making new cache")
                result = self._query_sample_otus()
                cache.set(key, result)
                logger.info("cached")
            else:
                logger.info("Using cached sample_otu results")
            return result
        else:
            result = self._query_sample_otus(otu_ids, sample_contextual_ids, use_union)
            return result

    def _query_sample_otus(self, otu_ids=None, sample_contextual_ids=None, use_union=None):
        '''
        Returns the sample_otu entries with the sample standardised count (between 0-1)
        '''
        # TODO: will need to make this more dynamic (queryable by sample id, count range)
        sample_otu_results = []

        query = (
            self._session.query(SampleOTU.otu_id, SampleOTU.sample_id, SampleOTU.proportional_abundance)
            .order_by(SampleOTU.otu_id)
        )

        if use_union is True:
            # sample otu needs to match EITHER the samples specified or the otus specified
            query = query.filter(or_(SampleOTU.otu_id.in_(otu_ids), SampleOTU.sample_id.in_(sample_contextual_ids)))
        else:
            # sample otu needs to match the samples specified AND the otus specified
            if len(otu_ids) > 0:
                query = query.filter(SampleOTU.otu_id.in_(otu_ids))
            if len(sample_contextual_ids) > 0:
                query = query.filter(SampleOTU.sample_id.in_(sample_contextual_ids))
            # same thing as below
            # query = query.filter(and_(SampleOTU.otu_id.in_(otu_ids), SampleOTU.sample_id.in_(sample_contextual_ids)))
        sample_otu_results = [r for r in query]
        return sample_otu_results

class EdnaPostImport:
    def __init__(self):
        self._session = Session()

    def __enter__(self):
        return self

    def __exit__(self, exec_type, exc_value, traceback):
        self._session.close()

    def _calculate_endemic_otus(self):
        '''
        gets all the otu_ids where they show in less than 1% of sites
        Query to get the distinct otu_ids to avoid repeating ids.
        '''
        logger.info("calculating endemic species")
        distinct_sample_count = len([r for r in self._session.query(SampleOTU.sample_id.distinct())]) 
        endemic_ids = [r[0] for r in (
            self._session.query(SampleOTU.otu_id, func.count(SampleOTU.sample_id))
            .group_by(SampleOTU.otu_id)
            .having(((func.count(SampleOTU.sample_id)) * 100) / distinct_sample_count < 1)
        )]
        # TODO: getting potentially false endemism results due to otu some otu entries being more general than others.
        # TODO: i.e. highly specific classification more likely to be considered endemic due to being seen as different species without accounting for how closely related species are
        for endemic_otu in self._session.query(OTU).filter(OTU.id.in_(endemic_ids)):
            endemic_otu.endemic = True;
        self._session.commit()

    def _normalize_abundances(self):
        '''
        Find un-standardised count data (i.e. counts below 1) and normalizes the value to be proportional to the sample total count.
        '''
        logger.info("calculating abundance proportions")
        # TODO: group by site, entry_abundance/total abundance -> 
        # TODO: just caching the sample maxes for now. Maybe in the future add it to a column 
        sample_totals_dict = { key: value for key, value in [r for r in (self._session.query(SampleOTU.sample_id, func.sum(SampleOTU.count)).group_by(SampleOTU.sample_id).filter(SampleOTU.count >= 1))]};
        for key, value in sample_totals_dict.items():
            for sample_otu in self._session.query(SampleOTU).filter(SampleOTU.sample_id == key):
                sample_otu.proportional_abundance = sample_otu.count / value
            self._session.commit()

    def _calculate_pathogenic_otus(self, import_base):
        '''
        compares to a list of pathogenic taxon classifications, if matches then sets pathogenic boolean to true
        '''

        def __contains_all_terms(code, classification):
            '''
            checks that all the term segments are in a code.
            '''
            for segment in classification:
                if segment in code:
                    continue
                else:
                    return False
            return True
                    
        logger.info("assigning pathogenic status")
        with open(import_base + 'edna/separated-data/pathogen_data/Potential_pathogens_list.txt', 'r') as f_input:
            lines = f_input.readlines()
            with open('./potential_pathogens.csv', 'w') as f_out:
                writer = csv.writer(f_out, delimiter=',')
                # first pass get headers
                pathogen_dict = {}
                classified_terms_list = []
                for line in lines[1:]:
                    header, entries = line.split(':')
                    for entry in entries.split(','):
                        entry.strip()
                        classification = entry.split()
                        if 'subsp.' in classification:
                            classification.remove('subsp.')
                        if 'pv.' in classification:
                            classification.remove('pv.')
                        classified_terms_list.append(classification)
            otus = [otu for otu in self._session.query(OTU)]
            pathogenic_ids = []
            for otu in otus:
                for classification in classified_terms_list:
                    if __contains_all_terms(otu.code, classification):
                        pathogenic_ids.append(otu.id)
                    else:
                        continue

            for otu in [r for r in self._session.query(OTU).filter(OTU.id.in_(pathogenic_ids))]:
                otu.pathogenic = True
            self._session.commit()

class ContextualFilter:
    mode_operators = {
        'or': or_,
        'and': sqlalchemy.and_,
    }

    def __init__(self, mode, environment_filter):
        self.mode = mode
        self.mode_func = ContextualFilter.mode_operators[self.mode]
        self.environment_filter = environment_filter
        self.terms = []

    def __repr__(self):
        return '<ContextualFilter(%s,env[%s],[%s]>' % (self.mode, repr(self.environment_filter), ','.join(repr(t) for t in self.terms))

    def add_term(self, term):
        self.terms.append(term)

    def apply(self, q):
        """
        return q with contextual filter terms applied to it
        """
        # if there's an environment filter, it applies prior to the filters
        # below, so it's outside of the application of mode_func
        q = apply_environment_filter(q, self.environment_filter)
        # chain together the conditions provided by each term,
        # combine into a single expression using our mode,
        # then filter the query
        return q.filter(
            self.mode_func(
                *(chain(*(t.conditions for t in self.terms)))))


class ContextualFilterTerm:
    def __init__(self, field_name, operator):
        self.field_name = field_name
        self.field = getattr(SampleContext, self.field_name)
        self.operator = operator

    @property
    def conditions(self):
        if self.operator in ('isnot', 'notbetween', 'containsnot'):
            return [sqlalchemy.not_(c) for c in (self.get_conditions())]
        return self.get_conditions()


class ContextualFilterTermFloat(ContextualFilterTerm):
    def __init__(self, field_name, operator, val_from, val_to):
        super().__init__(field_name, operator)
        assert(type(val_from) is float)
        assert(type(val_to) is float)
        self.val_from = val_from
        self.val_to = val_to

    def __repr__(self):
        return '<TermFloat(%s,%s,%s,%s)>' % (self.field_name, self.operator, self.val_from, self.val_to)

    def get_conditions(self):
        return [
            self.field.between(self.val_from, self.val_to)
        ]


class ContextualFilterTermDate(ContextualFilterTerm):
    def __init__(self, field_name, operator, val_from, val_to):
        super().__init__(field_name, operator)
        assert(type(val_from) is datetime.date)
        assert(type(val_to) is datetime.date)
        self.val_from = val_from
        self.val_to = val_to

    def __repr__(self):
        return '<TermDate(%s,%s,%s,%s)>' % (self.field_name, self.operator, self.val_from, self.val_to)

    def get_conditions(self):
        return [
            self.field.between(self.val_from, self.val_to)
        ]


class ContextualFilterTermString(ContextualFilterTerm):
    def __init__(self, field_name, operator, val_contains):
        super().__init__(field_name, operator)
        assert(type(val_contains) is str)
        self.val_contains = val_contains

    def __repr__(self):
        return '<TermString(%s,%s,%s)>' % (self.field_name, self.operator, self.val_contains)

    def get_conditions(self):
        cond = self.field.contains(self.val_contains)
        return [cond]


class ContextualFilterTermOntology(ContextualFilterTerm):
    def __init__(self, field_name, operator, val_is):
        super().__init__(field_name, operator)
        assert(type(val_is) is int)
        self.val_is = val_is

    def __repr__(self):
        return '<TermOntology(%s,%s,%s)>' % (self.field_name, self.operator, self.val_is)

    def get_conditions(self):
        return [
            self.field == self.val_is
        ]


class ContextualFilterTermSampleID(ContextualFilterTerm):
    def __init__(self, field_name, operator, val_is_in):
        super().__init__(field_name, operator)
        assert(type(val_is_in) is list)
        for t in val_is_in:
            assert(type(t) is int)
        self.val_is_in = val_is_in

    def __repr__(self):
        return '<TermSampleID(%s,%s,%s)>' % (self.field_name, self.operator, self.val_is_in)

    def get_conditions(self):
        return [
            self.field.in_(self.val_is_in)
        ]


def get_sample_ids():
    session = Session()
    ids = [t[0] for t in session.query(SampleContext.id).all()]
    session.close()
    return ids


def apply_op_and_val_filter(attr, q, op_and_val):
    if op_and_val is None or op_and_val.get('value') is None:
        return q
    value = op_and_val.get('value')
    if op_and_val.get('operator', 'is') == 'isnot':
        q = q.filter(attr != value)
    else:
        q = q.filter(attr == value)
    return q


def apply_otu_filter(otu_attr, q, op_and_val):
    return apply_op_and_val_filter(getattr(OTU, otu_attr), q, op_and_val)


apply_amplicon_filter = partial(apply_otu_filter, 'amplicon_id')
# w: applied quickfix to query
apply_environment_filter = partial(apply_op_and_val_filter, SampleContext.x)
