""" This module parses ex-factory excel file from PVM """

import os
import uuid
import weaviate
import time
import base64
from modules.utilities import find_property_by_name
from modules.utilities import generate_uuid
from modules.utilities import check_batch_result
from modules.utilities import DEFAULT_TYPES
from modules.utilities import VERBOSE

DEFAULT_MAX_BATCH = 1000


def _import_entities(instance: dict, client: weaviate.client, entities: dict):

    maxbatch = DEFAULT_MAX_BATCH
    if instance is not None and 'max_batch_size' in instance:
        maxbatch = instance['max_batch_size']

    totalcount = batchcount = 0
    batch = weaviate.ObjectsBatchRequest()
    for classname in entities:
        for name in entities[classname]:
            thing = {}
            thing['name'] = name
            newuuid = generate_uuid(classname, name)
            batch.add(thing, classname, newuuid)
            batchcount += 1
            totalcount += 1
            if batchcount >= maxbatch:
                print("Importing entities into Weaviate -----------", totalcount, end='\r')
                result = client.batch.create_objects(batch)
                batch = weaviate.ObjectsBatchRequest()
                batchcount = 0

    if batchcount > 0:
        result = client.batch.create_objects(batch)
        check_batch_result(result)
    print("Importing entities into Weaviate -----------", totalcount)


def _import_items(instance: dict, client: weaviate.client, schema: dict, data: dict, classname: str):

    maxbatch = DEFAULT_MAX_BATCH
    if instance is not None and 'max_batch_size' in instance:
        maxbatch = instance['max_batch_size']

    totalcount = batchcount = 0
    batch = weaviate.ObjectsBatchRequest()
    for item in data:
        thing = {}
        for key in item:
            prop = find_property_by_name(schema, key, classname)
            if prop is not None:
                if prop['dataType'][0] in DEFAULT_TYPES:
                    thing[key] = item[key]

        newuuid = generate_uuid(classname, item['identifier'])
        batch.add(thing, classname, newuuid)
        batchcount += 1
        totalcount += 1
        if batchcount >= maxbatch:
            print("Importing into Weaviate --------------------", totalcount, classname, end='\r')
            result = client.batch.create_objects(batch)
            batch = weaviate.ObjectsBatchRequest()
            batchcount = 0

    if batchcount > 0:
        result = client.batch.create_objects(batch)
        check_batch_result(result)
    print("Importing products into Weaviate -----------", totalcount, classname)


def _cross_reference(instance: dict, client: weaviate.client, data: list):

    maxbatch = DEFAULT_MAX_BATCH
    if instance is not None and 'max_batch_size' in instance:
        maxbatch = instance['max_batch_size']

    classname = "Relatie"
    batch = weaviate.ReferenceBatchRequest()
    count = 0
    for relatie in data:
        iuuid = generate_uuid(classname, relatie['identifier'])

        if 'heeftInteresses' in relatie:
            for name in relatie['heeftInteresses']:
                euuid = generate_uuid("Interesse", name)
                client.data_object.reference.add(iuuid, "heeftInteresses", euuid)
                client.data_object.reference.add(euuid, "vanRelatie", iuuid)
                count += 2
                print("Cross referencing entities -----------------", count, end='\r')

        if 'vanBedrijf' in relatie:
            for name in relatie['vanBedrijf']:
                euuid = generate_uuid("Bedrijf", name)
                client.data_object.reference.add(iuuid, "vanBedrijf", euuid)
                client.data_object.reference.add(euuid, "vanRelatie", iuuid)
                count += 2
                print("Cross referencing entities -----------------", count, end='\r')

        if 'heeftSchema' in relatie:
            for name in relatie['heeftSchema']:
                euuid = generate_uuid("Schema", name)
                client.data_object.reference.add(iuuid, "heeftSchema", euuid)
                client.data_object.reference.add(euuid, "vanRelatie", iuuid)
                count += 2
                print("Cross referencing entities -----------------", count, end='\r')
    print("Cross referencing entities -----------------", count)


def import_data(config: dict, client: weaviate.client, schema: dict, data: dict):

    instance = config['weaviate']
    _import_items(instance, client, schema, data['relaties'], "Relatie")
    _import_entities(instance, client, data['entities'])

    _cross_reference(instance, client, data['relaties'])
