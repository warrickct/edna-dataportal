
from django.core.management.base import BaseCommand
from ...importer import DataImporter
from ...cleaner import DataCleaner


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('base_dir', type=str)

    def handle(self, *args, **kwargs):
        importer = DataImporter(kwargs['base_dir'])
        cleaner = DataCleaner(kwargs['base_dir'])
        cleaner.remove_duplicate_sample_otus()
        site_lookup = importer.load_edna_contextual_metadata()
        otu_lookup = importer.load_edna_taxonomies()
        importer.load_edna_otu_abundance(otu_lookup, site_lookup)
