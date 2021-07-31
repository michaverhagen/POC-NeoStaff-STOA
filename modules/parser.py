""" This module parses datapoints from an different file types """

import os
import csv
import fnmatch

from modules.utilities import generate_uuid
from modules.utilities import remove_special_characters
from modules.utilities import find_property_by_column
from modules.utilities import convert_date_to_RFC3339


def _debug_print(data):
    #print(data)
    pass


def _parse_csv_file(path, datapoints, entities, schema, classname):
    headers = []
    count = 0
    if fnmatch.fnmatch(path, "*.csv"):
        with open(path, 'r') as readfile:
            reader = csv.reader(readfile, delimiter=',')
            if reader is not None:
                print("Parsing file:", path)
                for row in reader:
                    if count == 0:
                        headers = row
                    else:
                        item = {}
                        col = 0
                        for value in row:
                            if value is not None and value != '':
                                prop = find_property_by_column(schema, headers[col], classname)
                                if prop is not None:
                                    if prop['dataType'][0] == 'string':
                                        text = remove_special_characters(str(value))
                                        item[prop['name']] = text
                                    elif prop['dataType'][0] == 'text':
                                        text = remove_special_characters(str(value))
                                        item[prop['name']] = text
                                    elif prop['dataType'][0] == 'int':
                                        item[prop['name']] = int(value)
                                    elif prop['dataType'][0] == 'number':
                                        item[prop['name']] = float(value)
                                    elif prop['dataType'][0] == 'boolean':
                                        item[prop['name']] = bool(value)
                                    elif prop['dataType'][0] == 'date':
                                        date = convert_date_to_RFC3339(str(value))
                                        item[prop['name']] = date
                                    else:
                                        if prop['dataType'][0] not in entities:
                                            entities[prop['dataType'][0]] = []
                                        if str(value) not in entities[prop['dataType'][0]]:
                                            entities[prop['dataType'][0]].append(str(value))

                                        if prop['name'] not in item:
                                            item[prop['name']] = []
                                        item[prop['name']].append(str(value))
                                else:
                                    pass
                            col += 1
                        datapoints.append(item)
                    count += 1


def parse_data(datapath, schema):

    data = {}
    data['relaties'] = []
    data['entities'] = {}

    _parse_csv_file(datapath['basis'], data['relaties'], data['entities'], schema, "Relatie")

    _debug_print(data)

    return data
