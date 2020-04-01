import csv
from os import rename
from glob import glob
from collections import OrderedDict
import logging

logger = logging.getLogger("rainbow")

class DataCleaner:

    def __init__(self, import_base, **kwargs):
        self._import_base = import_base
        self._dev_test = False
        self._add_abundances = True


    def _sum_row_values(self, r, r2):
        ''' Where there is an OTU mentioned more than once per site, goes through that OTU's values, adds the row values together and returns summed row. '''
        new_row = []
        new_row.append(r[0])
        for index in range(1, len(r)):
            total = float(r[index]) + float(r2[index])
            if (float(r[index]) > 0 and float(r2[index]) > 0):
                print("non-zero values at site index: ", index)
            if (total.is_integer()):
                new_row.append(int(total))
            else:
                new_row.append(total)
        return new_row

    def remove_duplicate_sample_otus(self):
        ''' Combines abundance values for rows that contain the exact same OTU definition within a file.'''
        # TODO: Make the duplicate search apply across entire dataset or better yet, handle it within database entry rather than data pre-import cleaning.
        logger.info(self._import_base)
        for fname in sorted(glob(self._import_base + 'edna/data/*.tsv')):
            with open(fname, 'rU') as input_file:
                input_reader = csv.reader(input_file, delimiter='\t')
                headers = next(input_reader)
                rows_checked = 0
                duplicates_handled_count = 0
                otu_row_dict = OrderedDict()
                for otu_key, otu_row in enumerate(input_reader):
                    rows_checked += 1
                    otu_name = otu_row[0]
                    if otu_name in otu_row_dict:
                        if self._add_abundances:
                            print("duplicate of otu name: ", otu_name)
                            otu_row_dict[otu_name] = self._sum_row_values(otu_row_dict[otu_name], otu_row)
                        else:
                            otu_row_dict[otu_name] = otu_row
                        duplicates_handled_count += 1         
                    else:
                        otu_row_dict[otu_name] = otu_row
                        
            # move original files to new directory
            rename(fname, fname + '-original')
            with open(fname, "w+") as output_file:
                writer = csv.writer(output_file, delimiter="\t")
                writer.writerow(headers)
                for row in otu_row_dict:
                    # TODO: small casting of values to reduce file size ?
                    writer.writerow(otu_row_dict[row])
            print('Total rows combined: %d' % duplicates_handled_count)
            print('Total Rows checked: %d' % rows_checked)