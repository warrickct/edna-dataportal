from django.core.management.base import BaseCommand
from ...query import EdnaOTUQuery

from ...data_enhancer import DataEnhancer

import logging

logger = logging.getLogger("rainbow")

class Command(BaseCommand):


    def add_arguments(self, parser):
        parser.add_argument('base_dir', type=str)

    def handle(self, *args, **kwargs):
        logger.info("testing dev_test command")

        logger.info(kwargs)
        dd = DataEnhancer(kwargs['base_dir'])

        dd.enhance_data()
        # dd.combine_data()

        # DataEnhancer.using_csv()
        
        # ids = []
        # for i in range(4000):
        #     ids.append(i)

        # with EdnaOTUQuery() as query:
        #     # query.get_otu_pathogenic_status_by_id("code", ids)
        #     query._query_otu_primary_keys(['1'])
