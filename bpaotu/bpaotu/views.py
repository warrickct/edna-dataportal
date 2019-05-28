from collections import defaultdict, OrderedDict
import csv
import datetime
import io
import json
import logging
import re
import traceback
import zipstream

from django.conf import settings
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST
from django.views.generic import TemplateView
from django.http import JsonResponse, StreamingHttpResponse, HttpResponse

from .ckan_auth import require_CKAN_auth
from .importer import DataImporter
from .galaxy_client import Galaxy
from .otu import (
    Environment,
    OTUKingdom,
    SampleContext,
    )
from .query import (
    OTUQueryParams,
    TaxonomyOptions,
    OntologyInfo,
    SampleQuery,
    # w: Phase 3 edna API
    EdnaSampleContextualQuery,
    EdnaOTUQuery,
    EdnaSampleOTUQuery,
    # w: end
    ContextualFilter,
    ContextualFilterTermDate,
    ContextualFilterTermFloat,
    ContextualFilterTermOntology,
    ContextualFilterTermSampleID,
    ContextualFilterTermString,
    get_sample_ids)
from django.template import loader
from .models import (
    ImportFileLog,
    ImportOntologyLog,
    ImportSamplesMissingMetadataLog)
from .util import temporary_file

# TEST: For performance testing
import time

# TEST: for easy uploader
from django import forms

logger = logging.getLogger("rainbow")


# See datatables.net serverSide documentation for details
ORDERING_PATTERN = re.compile(r'^order\[(\d+)\]\[(dir|column)\]$')
COLUMN_PATTERN = re.compile(r'^columns\[(\d+)\]\[(data|name|searchable|orderable)\]$')

use_cors = True

def make_environment_lookup():
    with OntologyInfo() as info:
        return dict(info.get_values(Environment))


def format_bpa_id(int_id):
    return '102.100.100/%d' % int_id


def display_name(field_name):
    """
    a bit of a bodge, just replace '_' with ' ' and upper-case
    drop _id if it's there
    """
    if field_name.endswith('_id'):
        field_name = field_name[:-3]
    return ' '.join(((t[0].upper() + t[1:]) for t in field_name.split('_')))


class OTUSearch(TemplateView):
    template_name = 'bpaotu/search.html'
    ckan_base_url = settings.CKAN_SERVERS[0]['base_url']

    def get_context_data(self, **kwargs):
        context = super(OTUSearch, self).get_context_data(**kwargs)
        context['ckan_base_url'] = settings.CKAN_SERVERS[0]['base_url']
        context['ckan_auth_integration'] = settings.CKAN_AUTH_INTEGRATION

        return context


def int_if_not_already_none(v):
    if v is None or v == '':
        return None
    v = str(v)  # let's not let anything odd through
    return int(v)


def get_operator_and_int_value(v):
    if v is None or v == '':
        return None
    if v.get('value', '') == '':
        return None
    return OrderedDict((
        ('operator', v.get('operator', '=')),
        ('value', int_if_not_already_none(v['value'])),
    ))


clean_amplicon_filter = get_operator_and_int_value
clean_environment_filter = get_operator_and_int_value


def clean_taxonomy_filter(state_vector):
    """
    take a taxonomy filter (a list of phylum, kingdom, ...) and clean it
    so that it is a simple list of ints or None of the correct length.
    """

    assert(len(state_vector) == len(TaxonomyOptions.hierarchy))
    return list(map(
        get_operator_and_int_value,
        state_vector))


@require_CKAN_auth
@require_GET
def amplicon_options(request):
    """
    private API: return the possible amplicons
    """
    with OntologyInfo() as options:
        vals = options.get_values(OTUAmplicon)
    return JsonResponse({
        'possibilities': vals
    })


@require_CKAN_auth
@require_GET
def taxonomy_options(request):
    """
    private API: given taxonomy constraints, return the possible options
    """
    with TaxonomyOptions() as options:
        amplicon = clean_amplicon_filter(json.loads(request.GET['amplicon']))
        selected = clean_taxonomy_filter(json.loads(request.GET['selected']))
        possibilities = options.possibilities(amplicon, selected)
    return JsonResponse({
        'possibilities': possibilities
    })


@require_CKAN_auth
@require_GET
def contextual_fields(request):
    """
    private API: return the available fields, and their types, so that
    the contextual filtering UI can be built
    """
    fields_by_type = defaultdict(list)

    classifications = DataImporter.classify_fields(make_environment_lookup())

    ontology_classes = {}

    # group together columns by their type. note special case
    # handling for our ontology linkage columns
    for column in SampleContext.__table__.columns:
        if column.name == 'id':
            continue
        if hasattr(column, "ontology_class"):
            ty = '_ontology'
            ontology_classes[column.name] = column.ontology_class
        else:
            ty = str(column.type)
        fields_by_type[ty].append((column.name, getattr(column, 'units', None)))

    def make_defn(typ, name, units, **kwargs):
        environment = classifications.get(name)
        r = kwargs.copy()
        r.update({
            'type': typ,
            'name': name,
            'environment': environment
        })
        if units:
            r['units'] = units
        return r

    definitions = [make_defn('sample_id', 'id', None, display_name='Sample ID', values=list(sorted(get_sample_ids())))]
    for field_name, units in fields_by_type['DATE']:
        definitions.append(make_defn('date', field_name, units))
    for field_name, units in fields_by_type['FLOAT']:
        definitions.append(make_defn('float', field_name, units))
    for field_name, units in fields_by_type['CITEXT']:
        definitions.append(make_defn('string', field_name, units))
    with OntologyInfo() as info:
        for field_name, units in fields_by_type['_ontology']:
            ontology_class = ontology_classes[field_name]
            definitions.append(make_defn('ontology', field_name, units, values=info.get_values(ontology_class)))
    for defn in definitions:
        if 'display_name' not in defn:
            defn['display_name'] = display_name(defn['name'])

    definitions.sort(key=lambda x: x['display_name'])

    return JsonResponse({
        'definitions': definitions
    })


def param_to_filters(query_str):
    """
    take a JSON encoded query_str, validate, return any errors
    and the filter instances
    """

    def parse_date(s):
        try:
            return datetime.datetime.strptime(s, '%Y-%m-%d').date()
        except ValueError:
            return datetime.datetime.strptime(s, '%d/%m/%Y').date()

    def parse_float(s):
        try:
            return float(s)
        except ValueError:
            return None

    otu_query = json.loads(query_str)
    taxonomy_filter = clean_taxonomy_filter(otu_query['taxonomy_filters'])
    amplicon_filter = clean_amplicon_filter(otu_query['amplicon_filter'])

    context_spec = otu_query['contextual_filters']
    contextual_filter = ContextualFilter(context_spec['mode'], context_spec['environment'])

    errors = []

    for filter_spec in context_spec['filters']:
        field_name = filter_spec['field']
        if field_name not in SampleContext.__table__.columns:
            errors.append("Please select a contextual data field to filter upon.")
            continue
        operator = filter_spec.get('operator')
        column = SampleContext.__table__.columns[field_name]
        typ = str(column.type)
        try:
            if column.name == 'id':
                contextual_filter.add_term(ContextualFilterTermSampleID(field_name, operator, [int(t) for t in filter_spec['is']]))
            elif hasattr(column, 'ontology_class'):
                contextual_filter.add_term(
                    ContextualFilterTermOntology(field_name, operator, int(filter_spec['is'])))
            elif typ == 'DATE':
                contextual_filter.add_term(
                    ContextualFilterTermDate(field_name, operator, parse_date(filter_spec['from']), parse_date(filter_spec['to'])))
            elif typ == 'FLOAT':
                contextual_filter.add_term(
                    ContextualFilterTermFloat(field_name, operator, parse_float(filter_spec['from']), parse_float(filter_spec['to'])))
            elif typ == 'CITEXT':
                contextual_filter.add_term(
                    ContextualFilterTermString(field_name, operator, str(filter_spec['contains'])))
            else:
                raise ValueError("invalid filter term type: %s", typ)
        except Exception:
            errors.append("Invalid value provided for contextual field `%s'" % field_name)
            logger.critical("Exception parsing field: `%s':\n%s" % (field_name, traceback.format_exc()))

    return (OTUQueryParams(
        amplicon_filter=amplicon_filter,
        contextual_filter=contextual_filter,
        taxonomy_filter=taxonomy_filter), errors)


def param_to_filters_without_checks(query_str):
    otu_query = json.loads(query_str)
    taxonomy_filter = clean_taxonomy_filter(otu_query['taxonomy_filters'])
    amplicon_filter = clean_amplicon_filter(otu_query['amplicon_filter'])

    context_spec = otu_query['contextual_filters']
    contextual_filter = ContextualFilter(context_spec['mode'], context_spec['environment'])

    errors = []

    return (OTUQueryParams(
        amplicon_filter=amplicon_filter,
        contextual_filter=contextual_filter,
        taxonomy_filter=taxonomy_filter), errors)

@csrf_exempt
@require_GET
def test(request):
    return JsonResponse({
        'response': request
    })


# TEST: Adding custom API for the visualisation
@csrf_exempt
@require_GET
def edna_get_sample_otu(request):
    '''
    Returns sample_otu entries from otu table combination-keys
    '''

    # FIXME: otu filters applied subtractively, contextual filters applied additively.

    # Sample Contexts
    sample_contextual_ids = []
    contextual_params = request.GET.getlist('q', None)

    # check for password
    password = None
    for param in contextual_params:
        if "password" in param:
            password = param
    logger.info(password)

    # just the primary keys for querying
    # the sample data for plotting geographically etc.
    sample_contextuals_data = []
    with EdnaSampleContextualQuery() as sample_contextual:
        if len(contextual_params) > 0:
            sample_contextuals_data = sample_contextual.query_sample_contextuals(contextual_params, password)
        else:
            sample_contextuals_data = sample_contextual.query_sample_contextuals()
        sample_contextual_ids = [sample['id'] for sample in sample_contextuals_data]

    # OTUs

    # Retrieving fk combination keys from request
    otu_ids = []
    otu_taxonomic_ids = [otu for otu in request.GET.getlist('otu') if otu is not '']

    # Getting text if there is any
    otu_texts = [otu for otu in request.GET.getlist('text') if otu is not '']
    logger.info(otu_texts)
    if otu_texts:
        logger.info("otu texts true")
    else:
        logger.info("otu texts false")

    # if endemism value exists in request then query will include it.
    endemic_value = request.GET.get('endemic', None) == "true" 
    use_endemism = False
    if endemic_value is not None:
        use_endemism = True

    with EdnaOTUQuery() as otu_query:
        otu_ids = otu_query._query_otu_primary_keys(otu_combination_keys=otu_taxonomic_ids, otu_terms=otu_texts, use_endemism=use_endemism, endemic_value=endemic_value)
        # otu ids sorted by pathogenic status
        # TODO: fix no pathogen ids and no otu ids when no filter params.
        pathogenic_otu_ids = otu_query.get_otu_pathogenic_status_by_id(otu_ids)

    use_union = request.GET.get('operator', None) == "union" 
    # Combining OTU id sets with Contextual sets to query Abundance table
    with EdnaSampleOTUQuery() as sample_otu_query:
        # Getting the sample otu entries that are within either otu_id set or sample_contextual_id set.
        sample_otu_results = sample_otu_query.query_sample_otus(otu_ids, sample_contextual_ids, use_union)
    # Filter to only include sample contextual data that is included in sample otu result set.
    contextual_ids_in_sampleotu_results = set([so[1] for so in sample_otu_results])

    # grabbing contextual data for sites included in sample_otu result set
    sample_contextuals_data = [sc for sc in sample_contextuals_data if (sc['id'] in contextual_ids_in_sampleotu_results)]
    response = JsonResponse({
        'sample_otu_data': sample_otu_results,
        'sample_contextual_data': sample_contextuals_data,
        'pathogenic_otus': pathogenic_otu_ids
    })

    # TODO: response['Access-Control-Allow-Origin'] =   'http://localhost:5500/'
    # response header is set by apache to '*' on the nectar edna virtual machine so this is no longer needed
    # TODO: make cors more restricted potentially
    # TODO: configure docker to automatically control cors settings. (will require altering nginx configuration for docker)

    if use_cors:
        response['Access-Control-Allow-Origin'] = '*'
    return response

@csrf_exempt
@require_GET
def edna_otu(request, id=None):
    '''
    returns otu table information.
    '''
    # should improve the api structure. otu/{id}/kingdom/{kingdom id}/etc...
    logger.info(id)

    otu_ids = request.GET.getlist('id', None)
    if len(otu_ids) > 0:
        with EdnaOTUQuery() as query:
            otu_names = query.get_otu_names(otu_ids)
        response = JsonResponse({
            'otu_names': otu_names
        })
    else:
        response = JsonResponse({
            'otu_names': []
        })

    if id:
        with EdnaOTUQuery() as q:
            otu_code = q.get_otu(id)
            response = JsonResponse({
                'otu_names': otu_code
            })

    if use_cors:    
        response['Access-Control-Allow-Origin'] = '*'
    return response

@csrf_exempt
@require_GET
def edna_filter_options(request):
    '''
    Calls both meta and otu filter option methods, combined and returns.
    '''

    def _paginate_results(result, page=1, page_size=50):
        start = ((page -1) * page_size)
        end = ((page -1) * page_size) + page_size
        paginated_result = result[start:end]
        return paginated_result

    def _filter_results(results, filters=None):
        if filters is not None:
            if filters:
                filters = filters.lower()
            # filtering by text string, r[0]
            results = [r for r in results if (filters in r[0].lower())]
        logger.info(results)
        return results

    filters = request.GET['q']
    page = int(request.GET['page'])
    page_size = int(request.GET['page_size'])
    with EdnaOTUQuery() as query:
        otu_suggestions = query.get_taxonomy_options()
        total_otu_suggestions = len(otu_suggestions)
        paginated_otu_suggestions = _paginate_results(_filter_results(otu_suggestions, filters))
    with EdnaSampleContextualQuery() as query:
        context_options = query.get_sample_contextual_options(filters)
    # combined_options = taxonomy_options + context_options
    response = JsonResponse({
        'data': {
            'total_results': total_otu_suggestions,
            'taxonomy_options': paginated_otu_suggestions,
            'context_options': context_options,
        }
    })
    if use_cors:    
        response['Access-Control-Allow-Origin'] = '*'
    response['Access-Control-Allow-Headers'] = 'Content-Type'
    return response

@csrf_exempt
@require_GET
def edna_sample_contextual(request, id=None):
    logger.info("calling enda sample_context")
    with EdnaSampleContextualQuery() as q:
        site_name = q.get_sample_context_entry(id)
    response = JsonResponse({
        'name': site_name
    })
    if use_cors:
        response['Access-Control-Allow-Origin'] = '*'
    response['Access-Control-Allow-Headers'] = 'Content-Type'
    return response


@csrf_exempt
@require_GET
def edna_suggestions_2(request):
    kingdom = request.GET.get('kingdom', None)
    phylum = request.GET.get('phylum', None)
    klass = request.GET.get('class', None)
    order = request.GET.get('order', None)
    family = request.GET.get('family', None)
    genus = request.GET.get('genus', None)
    species = request.GET.get('species', None)

    print("%s " % kingdom)
    # print("%s %s %s %s %s %s %s " % kingdom, phylum, klass, order, family, genus, species)

    with EdnaOTUQuery() as q:
        results = q.get_taxonomy_options()

        tree ={}
        for r in results:
            level = tree
            # logger.info(r)
            fks = r[1]
            for index, fk in enumerate(fks):
                if fk in level:
                    level = level[fk]
                else:
                    level[fk] = {
                        'text': r[0].split(';')[index]
                    }
        # logger.info(tree.keys())
        # logger.info(tree['k__Fungi']['_id'])
    # logger.info(tree)

    suggestions = []
    taxons = [kingdom, phylum, klass, order, family, genus, species]
    level = tree
    for t in taxons:
        # logger.info(t)
        if t:
            t_id = int(t)
            if t_id in level:
                level = level[t_id]
                logger.info(level.keys())
            else:
                raise KeyError('taxon id not found')
        else:
            # grab all within level
            for k, v in level.items():
                if k == "text":
                    continue
                # logger.info(k)
                # logger.info(v)
                suggestion = {
                    'id': k,
                    'text': v['text']
                }
                suggestions.append(suggestion)
            break
    # logger.info(suggestions)

    # return HttpResponse("<h1>"+ response +"</h1>")
    
    response =  JsonResponse({
        'suggestions': suggestions
    })
    if use_cors:    
        response['Access-Control-Allow-Origin'] = '*'
    response['Access-Control-Allow-Headers'] = 'Content-Type'
    return response


@csrf_exempt
@require_GET
# def edna_contextual_suggestions(contextual_field=None):
def edna_contextual_suggestions(request, context_field):
    logger.info(context_field)
    with EdnaSampleContextualQuery() as q:
        distinct_field_values = q.query_distinct_field_values(context_field)
    response = JsonResponse({
        'data': distinct_field_values
    })
    if use_cors:    
        response['Access-Control-Allow-Origin'] = '*'
    response['Access-Control-Allow-Headers'] = 'Content-Type'
    return response

# TEMP:TEST: API class made for easier uploading.
class UploadFileForm(forms.Form):
    file = forms.FileField()


class AbundanceUpload(TemplateView):
    # make an html template for the upload page.
    template_name = 'edna/upload.html'

    def get_context_data(self, **kwargs):
        context = super(AbundanceUpload, self).get_context_data(**kwargs)
        logger.info(context)
        context['ckan_base_url'] = settings.CKAN_SERVERS[0]['base_url']
        context['ckan_auth_integration'] = settings.CKAN_AUTH_INTEGRATION
        return context

    def handle_uploaded_file(self, f):
        logger.info("running handle uploaded file")
        reader = csv.reader(f, delimiter="\t")
        for line in reader:
            logger.info(line)

    def post(self, request):
        
        return HttpResponse("post method")

@csrf_exempt
@require_POST
def required_table_headers(request):
    """
    This is a modification of the otu_search method to populate the DataTable
    """

    def _int_get_param(param_name):
        param = request.POST.get(param_name)
        try:
            return int(param) if param is not None else None
        except ValueError:
            return None

    draw = _int_get_param('draw')
    start = _int_get_param('start')
    length = _int_get_param('length')

    search_terms = json.loads(request.POST['otu_query'])

    order_col = request.POST['order[0][column]']
    order_type = request.POST['order[0][dir]']

    contextual_terms = search_terms['contextual_filters']['filters']

    required_headers = []
    for elem in contextual_terms:
        required_headers.append(elem['field'])

    results = []
    result_count = len(results)

    environment_lookup = make_environment_lookup()

    params, errors = param_to_filters_without_checks(request.POST['otu_query'])
    with SampleQuery(params) as query:
        results = query.matching_sample_headers(required_headers, order_col, order_type)

    result_count = len(results)
    results = results[start:start + length]

    def get_environment(environment_id):
        if environment_id is None:
            return None
        return environment_lookup[environment_id]

    data = []
    for t in results:
        count = 2
        # TODO: Need to make this return something more useful for the eDNA webapp possibly.
        # TODO: does not need changing for the visualisation integration. May need changing for data portal searching.
        # NOTE: Returns {"bpa_id": 7032, "environment": "Soil"}
        data_dict = {"bpa_id": t[0], "environment": get_environment(t[1])}

        for rh in required_headers:
            data_dict[rh] = t[count]
            count = count + 1

        data.append(data_dict)

    res = {
        'draw': draw,
    }
    res.update({
        'data': data,
        'recordsTotal': result_count,
        'recordsFiltered': result_count,
    })
    return JsonResponse(res)


@csrf_exempt
@require_CKAN_auth
@require_POST
def otu_search_sample_sites(request):
    params, errors = param_to_filters(request.POST['otu_query'])
    if errors:
        return JsonResponse({
            'errors': [str(e) for e in errors],
            'data': [],
        })

    with SampleQuery(params) as query:
        results = query.matching_samples()

    def format(sample):
        return {
            'bpa_id': sample.id,
            'latitude': sample.latitude,
            'longitude': sample.longitude,
        }

    return JsonResponse({
        'data': [format(sample) for sample in results]
    })


# technically we should be using GET, but the specification
# of the query (plus the datatables params) is large: so we
# avoid the issues of long URLs by simply POSTing the query
@csrf_exempt
@require_CKAN_auth
@require_POST
def otu_search(request):
    """
    private API: return the available fields, and their types, so that
    the contextual filtering UI can be built
    """
    def _int_get_param(param_name):
        param = request.POST.get(param_name)
        try:
            return int(param) if param is not None else None
        except ValueError:
            return None

    draw = _int_get_param('draw')
    start = _int_get_param('start')
    length = _int_get_param('length')

    environment_lookup = make_environment_lookup()

    params, errors = param_to_filters(request.POST['otu_query'])
    with SampleQuery(params) as query:
        results = query.matching_sample_ids_and_environment()
    result_count = len(results)
    results = results[start:start + length]

    def get_environment(environment_id):
        if environment_id is None:
            return None
        return environment_lookup[environment_id]

    res = {
        'draw': draw,
    }
    if errors:
        res.update({
            'errors': [str(t) for t in errors],
            'data': [],
            'recordsTotal': 0,
            'recordsFiltered': 0,
        })
    else:
        res.update({
            # w: Removed the environment_id from the results.
            'data': [{"bpa_id": t[0]} for t in results],
            'recordsTotal': result_count,
            'recordsFiltered': result_count,
        })
    return JsonResponse(res)


def contextual_csv(samples):
    with OntologyInfo() as info:
        def make_ontology_export(ontology_cls):
            values = dict(info.get_values(ontology_cls))

            def __ontology_lookup(x):
                if x is None:
                    return ''
                return values[x]
            return __ontology_lookup

        def str_none_blank(v):
            if v is None:
                return ''
            return str(v)

        csv_fd = io.StringIO()
        w = csv.writer(csv_fd)
        fields = []
        heading = []
        write_fns = []
        for column in SampleContext.__table__.columns:
            fields.append(column.name)
            units = getattr(column, 'units', None)
            if column.name == 'id':
                heading.append('BPA ID')
            else:
                title = display_name(column.name)
                if units:
                    title += ' [%s]' % units
                heading.append(title)

            if column.name == 'id':
                write_fns.append(format_bpa_id)
            elif hasattr(column, "ontology_class"):
                write_fns.append(make_ontology_export(column.ontology_class))
            else:
                write_fns.append(str_none_blank)
        w.writerow(heading)
        for sample in samples:
            w.writerow(f(getattr(sample, field)) for (field, f) in zip(fields, write_fns))
        return csv_fd.getvalue()


@require_CKAN_auth
@require_GET
def otu_export(request):
    """
    this view takes:
     - contextual filters
     - taxonomic filters
    produces a Zip file containing:
      - an CSV of all the contextual data samples matching the query
      - an CSV of all the OTUs matching the query, with counts against Sample IDs
    """
    def val_or_empty(obj):
        if obj is None:
            return ''
        return obj.value

    zf = zipstream.ZipFile(mode='w', compression=zipstream.ZIP_DEFLATED)
    params, errors = param_to_filters(request.GET['q'])
    with SampleQuery(params) as query:
        def sample_otu_csv_rows(kingdom_id):
            fd = io.StringIO()
            w = csv.writer(fd)
            w.writerow([
                'BPA ID',
                'OTU',
                'OTU Count',
                'Amplicon',
                'Kingdom',
                'Phylum',
                'Class',
                'Order',
                'Family',
                'Genus',
                'Species'])
            yield fd.getvalue().encode('utf8')
            fd.seek(0)
            fd.truncate(0)
            q = query.matching_sample_otus(kingdom_id)
            for i, (otu, sample_otu, sample_context) in enumerate(q.yield_per(50)):
                w.writerow([
                    format_bpa_id(sample_otu.sample_id),
                    otu.code,
                    sample_otu.count,
                    val_or_empty(otu.amplicon),
                    val_or_empty(otu.kingdom),
                    val_or_empty(otu.phylum),
                    val_or_empty(otu.klass),
                    val_or_empty(otu.order),
                    val_or_empty(otu.family),
                    val_or_empty(otu.genus),
                    val_or_empty(otu.species)])
                yield fd.getvalue().encode('utf8')
                fd.seek(0)
                fd.truncate(0)

        zf.writestr('contextual.csv', contextual_csv(query.matching_samples()).encode('utf8'))
        with OntologyInfo() as info:
            for kingdom_id, kingdom_label in info.get_values(OTUKingdom):
                if not query.has_matching_sample_otus(kingdom_id):
                    continue
                zf.write_iter('%s.csv' % (kingdom_label), sample_otu_csv_rows(kingdom_id))

    response = StreamingHttpResponse(zf, content_type='application/zip')
    filename = "BPASearchResultsExport.zip"
    response['Content-Disposition'] = 'attachment; filename="%s"' % filename
    return response


@csrf_exempt
@require_CKAN_auth
@require_POST
def submit_to_galaxy(request):
    WORKFLOW_NAME = 'Hello world'
    HELLO_CONTENTS = 'Hello, world!\nGoodbye, world!\n'
    HISTORY_NAME = 'Hello World'

    ckan_data = request.ckan_data
    galaxy = Galaxy()

    def _get_users_galaxy_api_key(email):
        galaxy_user = galaxy.users.get_by_email(email)
        if galaxy_user is None:
            galaxy_user = galaxy.users.create(email)

        api_key = galaxy.users.get_api_key(galaxy_user['id'])
        if api_key is None:
            api_key = galaxy.users.create_api_key(galaxy_user['id'])
        return api_key

    def _handle():
        workflow = galaxy.workflows.get_by_name(WORKFLOW_NAME)
        if workflow is None:
            raise Exception("Couldn't find workflow '%s' for current user" % WORKFLOW_NAME)

        email = ckan_data.get('email')
        if not email:
            raise Exception("Could not retrieve user's email")

        users_api_key = _get_users_galaxy_api_key(email)

        users_galaxy = Galaxy(api_key=users_api_key)

        history = users_galaxy.histories.create(HISTORY_NAME)

        with temporary_file(HELLO_CONTENTS) as path:
            file_id = users_galaxy.histories.upload_file(history.get('id'), path, filename='hello_world.txt')
            users_galaxy.histories.wait_for_file_upload_to_finish(history.get('id'), file_id)

        wfl = users_galaxy.workflows.submit(
            workflow_id=workflow.get('id'),
            history_id=history.get('id'),
            file_ids={'0': file_id})

        wfl_data = {k: v for k, v in wfl.items() if k in ('workflow_id', 'history', 'state')}
        return {
            'success': True,
            'workflow': wfl_data,
        }

    try:
        response = _handle()
    except Exception as exc:
        logger.exception('Error in submit to Galaxy')
        response = {
            'success': False,
            'errors': str(exc),
        }

    return JsonResponse(response)


def otu_log(request):
    template = loader.get_template('bpaotu/otu_log.html')
    missing_sample_ids = []
    from .query import Session
    from .otu import (SampleContext, OTU, SampleOTU)
    for obj in ImportSamplesMissingMetadataLog.objects.all():
        missing_sample_ids += obj.samples_without_metadata
    session = Session()
    context = {
        'files': ImportFileLog.objects.all(),
        'ontology_errors': ImportOntologyLog.objects.all(),
        'missing_samples': ', '.join(sorted(missing_sample_ids)),
        'otu_count': session.query(OTU).count(),
        'sampleotu_count': session.query(SampleOTU).count(),
        'samplecontext_count': session.query(SampleContext).count(),
    }
    session.close()
    return HttpResponse(template.render(context, request))


@csrf_exempt
@require_POST
def contextual_csv_download_endpoint(request):
    data = request.POST.get('otu_query')

    search_terms = json.loads(data)
    contextual_terms = search_terms['contextual_filters']['filters']

    required_headers = []
    for h in contextual_terms:
        required_headers.append(h['field'])

    params, errors = param_to_filters_without_checks(request.POST['otu_query'])
    with SampleQuery(params) as query:
        results = query.matching_sample_headers(required_headers)

    header = ['sample_bpa_id', 'bpa_project'] + required_headers

    file_buffer = io.StringIO()
    csv_writer = csv.writer(file_buffer)

    def read_and_flush():
        data = file_buffer.getvalue()
        file_buffer.seek(0)
        file_buffer.truncate()
        return data

    def yield_csv_function():
        csv_writer.writerow(header)
        yield read_and_flush()

        for r in results:
            row = []
            row.append(r)

            csv_writer.writerow(r)
            yield read_and_flush()

    response = StreamingHttpResponse(yield_csv_function(), content_type="text/csv")
    response['Content-Disposition'] = "application/download; filename=table.csv"

    return response


@csrf_exempt
def tables(request):
    template = loader.get_template('bpaotu/tables.html')
    context = {
        'ckan_auth_integration': settings.CKAN_AUTH_INTEGRATION
    }

    return HttpResponse(template.render(context, request))
