
from django.core.management.base import BaseCommand
from ...importer import DataImporter


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('base_dir', type=str)

    def handle(self, *args, **kwargs):
        importer = DataImporter(kwargs['base_dir'])
        # importer.load_soil_contextual_metadata()
        # importer.load_marine_contextual_metadata()
        site_lookup = importer.load_waterdata_contextual_metadata()
        # otu_lookup = importer.load_taxonomies()
        otu_lookup = importer.load_waterdata_taxonomies()
        # importer.load_otu_abundance(otu_lookup)
        importer.load_waterdata_otu_abundance(otu_lookup, site_lookup)
