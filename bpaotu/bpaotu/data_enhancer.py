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

    def using_shape():
        for coord in sample_context_file_iterator():
            print(coord)
        df = gpd.read_file('./nzlri-soil.shp')
        for row in df.iterrows():
            print(row)

    def iterate_csv(self, path):
        csv.field_size_limit(sys.maxsize)
        with open(path, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file, delimiter=',')
            for row in reader:
                yield row

    def sample_context_file_iterator():
        for row in iterate_csv('./p7_sarah_thompson.csv'):
            try:
                point = Point(float(row['Longitude']), float('-' + row['Latitude']))
            except:
                continue
            yield point

    def polygon_file_iterator():
        for row in iterate_csv('./nzlri-soil.csv'):
            poly = wkt.loads(row['WKT'])
            yield poly

    def make_poly_list():
        print("making poly list...")
        casted_poly_list = []
        for row in iterate_csv('./nzlri-soil.csv'):
            poly = wkt.loads(row['WKT'])
            row['poly'] = poly
            casted_poly_list.append(row)
        return casted_poly_list

    def make_sample_list(self):
        print("making sample point lookup")
        samples_list = []
        for row in self.iterate_csv('./p7_sarah_thompson.csv'):
            point = Point(float(row['Longitude']), float('-' + row['Latitude']))
            row['point'] = point
            samples_list.append(row)
        return samples_list

    def add_fields_to_dict(dict1, dict2, string):
        for key, value in dict2.items():
            if string in key.lower():
                print("adding: " + key)
                dict1[key] = value
        return dict1

    def alter_sample_properties(sample, poly_tuple):
        new_sample = add_fields_to_dict(sample, poly_tuple, "soil")
        del new_sample['point']
        return new_sample

    def combine_data(self):
        sample_list = self.make_sample_list()
        p_list = make_poly_list()
        for sample in sample_list:
            for poly_tuple in p_list:
                if poly_tuple['poly'].contains(sample['point']):
                    new_sample = alter_sample_properties(sample, poly_tuple)
                    print(new_sample)
                    break
