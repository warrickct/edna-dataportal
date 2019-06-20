# spatial_index = gdf.sindex
# possible_matches_index = list(spatial_index.intersection(polygon.bounds))
# possible_matches = gdf.iloc[possible_matches_index]
# precise_matches = possible_matches[possible_matches.intersects(polygon)]

# TODO: Look into R-tree
# https://geoffboeing.com/2016/10/r-tree-spatial-index-python/
# Used for quickly finding the closest possible polygon

# https://automating-gis-processes.github.io/2016/Lesson3-point-in-polygon.html
# ^ Checks if a point is literally within a polygon

# import glob
# import logging
# import geopandas as gpd
# import fiona
import sys
import csv
from shapely.geometry import Point, Polygon
from shapely import wkt

class DataEnhancer:

    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.sample_dir = self.base_dir + 'edna/separated-data/metadata/p7_sarah_thompson.csv'
        self.geographic_dir = self.base_dir + 'edna/geographic-data/nzlri-soil.csv'

    # def using_shape():
    #     for coord in sample_context_file_iterator():
    #         print(coord)
    #     df = gpd.read_file('./nzlri-soil.shp')
    #     for row in df.iterrows():
    #         print(row)


    # def sample_context_file_iterator():
    #     for row in iterate_csv('edna/separated-data/data/p7_sarah_thompson.csv'):
    #         try:
    #             point = Point(float(row['Longitude']), float('-' + row['Latitude']))
    #         except:
    #             continue
    #         yield point

    def insert_str_at(self, file_path, insert_str, index_substr):
        idx = file_path.rfind(index_substr)
        return file_path[:idx] + insert_str + file_path[idx:]

    def iterate_csv(self, path):
        # edna/separated-data/data/p7_sarah_thompson.csv
        csv.field_size_limit(sys.maxsize)
        with open(path, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file, delimiter=',')
            for row in reader:
                yield row

    def polygon_file_iterator():
        for row in iterate_csv('edna/geographic-data/nzlri-soil.csv'):
            poly = wkt.loads(row['WKT'])
            yield poly

    def make_poly_list(self):
        print("making poly list...")
        casted_poly_list = []
        for row in self.iterate_csv('edna/geographic-data/nzlri-soil.csv'):
            poly = wkt.loads(row['WKT'])
            row['poly'] = poly
            casted_poly_list.append(row)
        return casted_poly_list

    def make_sample_list(self):
        print("making sample point lookup")
        samples_list = []
        for row in self.iterate_csv(self.base_dir + 'edna/separated-data/metadata/p7_sarah_thompson.csv'):
            point = Point(float(row['Longitude']), float('-' + row['Latitude']))
            row['point'] = point
            samples_list.append(row)
        return samples_list

    def add_fields_to_dict(self, sample, soil_tuple, string):
        for key, value in soil_tuple.items():
            if string in key.lower():
                # print("adding: " + key)
                if value == '':
                    value = 'unclassified'
                sample[key] = value
        return sample

    def alter_sample_properties(self, sample, poly_tuple):
        new_sample = self.add_fields_to_dict(sample, poly_tuple, "soil")
        del new_sample['point']
        return new_sample

    def create_enhanced_sample_list(self):
        sample_list = self.make_sample_list()
        p_list = self.make_poly_list()
        enhanced_sample_list = []
        for sample in sample_list:
            for poly_tuple in p_list:
                if poly_tuple['poly'].contains(sample['point']):
                    enhanced_sample = self.alter_sample_properties(sample, poly_tuple)
                    enhanced_sample_list.append(enhanced_sample)
                    break
        return enhanced_sample_list

    # def enhance_data2(self):
    #     f = open("dict_test", 'w+')
    #     writer = csv.DictWriter(f, ['1', '2'])
    #     writer.writeheader()
    #     writer.writerow({
    #         '1': 'test',
    #         '2': 'test2',
    #     })
    #     f.close()
    
    def enhance_data(self):
        output_file_path = self.insert_str_at(self.sample_dir, '/enhanced', '/')
        # print('output file in : %s' % output_file_path)
        # f = open("dict_test", 'w+')
        f = open(output_file_path, 'w+')
        new_samples = self.create_enhanced_sample_list()
        fieldnames = new_samples[0].keys()
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for sample in new_samples:
            writer.writerow(sample)
        f.close()