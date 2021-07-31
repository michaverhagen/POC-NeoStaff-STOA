#!/usr/bin/env python3
""" load Galley data """

import time
import yaml
from modules.schema import create_schema
from modules.schema import load_schema
from modules.parser import parse_data
from modules.imports import import_data
from modules.utilities import get_weaviate_client


DATA_CONFIG = "./data/data_configuration.xlsx"


def _load_data():
    """ load the data """

    with open('./config.yml') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
        schema = create_schema(config['weaviate'], config['data']['config'])
        data = parse_data(config['data'], schema)

        client = get_weaviate_client(config['weaviate'])
        load_schema(config['weaviate'], client)
        import_data(config, client, schema, data)


#########################################################################################################
# only the call for the main function below this line
#########################################################################################################


def main():
    """ main """
    start = time.time()

    _load_data()

    end = time.time()
    minutes = round((end-start)/60)
    print("Total time required ------------------------:", minutes, "minutes", round((end-start)%60, 1), "seconds")


if __name__ == '__main__':
    main()
