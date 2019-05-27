from django.conf.urls import url
from django.contrib import admin
from django.conf import settings
from django.conf.urls.static import static
from . import views

admin.autodiscover()

# w:TODO: Need to make a custom API pattern for getting the full otu.code, sample_context._site thing and the sample_otu.value.
urlpatterns = [
    url(r'^$', views.OTUSearch.as_view()),
    url(r'^private/api/v1/amplicon-options$', views.amplicon_options, name="amplicon_options"),
    url(r'^private/api/v1/taxonomy-options$', views.taxonomy_options, name="taxonomy_options"),
    url(r'^private/api/v1/contextual-fields$', views.contextual_fields, name="contextual_fields"),
    url(r'^private/api/v1/search$', views.otu_search, name="otu_search"),
    url(r'^private/api/v1/search-sample-sites$', views.otu_search_sample_sites, name="otu_search_sample_sites"),
    url(r'^private/api/v1/submit_to_galaxy$', views.submit_to_galaxy, name="submit_to_galaxy"),
    url(r'^private/api/v1/export$', views.otu_export, name="otu_export"),

    # eDNA endpoints.
    # returns sample_otu entries

    # http://localhost:8000/edna/api/v1.0/abundance?otu=&q=elevation$gt1&q=elevation$lt100&operator=intersection
    url(r'^edna/api/v1.0/abundance$', views.edna_get_sample_otu, name="edna_sample_otus"),
    # returns filter options list
    url(r'^edna/api/v1.0/filter-options$', views.edna_filter_options, name="edna_filter_options"),

    url(r'^edna/api/v1.0/otu/(?P<id>[0-9]*)', views.edna_otu, name="edna_otu"),

    url(r'^edna/api/v1.0/sample_context/(?P<id>[0-9]*)', views.edna_sample_contextual, name="edna_sample_context"),

    url(r'^edna/api/v1.0/suggestions/', views.edna_suggestions_2, name="edna_suggestions_2"),

    url(r'^edna/api/v1.0/suggestions/', views.edna_suggestions_2, name="edna_suggestions_2"),
    
    url(r'^edna/api/v1.0/context-suggestions/(?P<context_field>[a-zA-Z\d_]*)', views.edna_contextual_suggestions, name="edna_contextual_suggestions"),
    
    # TODO: WIP: For posting new data sets.
    # url(r'^edna/api/upload$', views.AbundanceUpload.as_view()),
    # url(r'^edna/api/dev/test/$', views.test, name="Test stub method"),

    url(r'^tables/$', views.tables, name="tables"),                                                                                 # Custom datatables columns.
    url(r'^private/api/v1/required_table_headers/$', views.required_table_headers, name="required_table_headers"),                  # Custom datatables columns.
    url(r'^contextual_csv_download_endpoint/$', views.contextual_csv_download_endpoint, name="contextual_csv_download_endpoint"),   # Custom datatables columns.
] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
