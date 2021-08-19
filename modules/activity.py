""" Check activity """

import datetime
import weaviate
from dateutil.parser import parse
from modules.query import get_relaties
from modules.utilities import get_weaviate_client
from modules.utilities import convert_datetime_to_RFC3339


def _check_contact(config, relatie, today):

    client = get_weaviate_client(config['weaviate'])
    contact = False
    if 'contactDatum' in relatie and relatie['contactDatum'] is not None:
        date = parse(relatie['contactDatum'])
        naive = date.replace(tzinfo=None)
        if (today-naive).days > 30:
            contact = True
    else:
        contact = True

    if contact:
        update = {}
        update['contactDatum'] = convert_datetime_to_RFC3339(str(today))
        client.data_object.update(update, "Relatie", relatie['_additional']['id'])
        print(relatie['voornaam'], relatie['achternaam'], "heeft contact")


def _check_anniversary(config, relatie, today):

    client = get_weaviate_client(config['weaviate'])
    if 'startDatum' in relatie and relatie['startDatum'] is not None:
        date = parse(relatie['startDatum'])
        if today.day == date.day and today.month == date.month:
            print(relatie['voornaam'], relatie['achternaam'], "heeft anniversary:", today.year-date.year)
            update = {}
            update['contactDatum'] = convert_datetime_to_RFC3339(str(today))
            client.data_object.update(update, "Relatie", relatie['_additional']['id'])


def _check_birthday(config, relatie, today):

    client = get_weaviate_client(config['weaviate'])
    if 'geboorteDatum' in relatie and relatie['geboorteDatum'] is not None:
        date = parse(relatie['geboorteDatum'])
        if today.day == date.day and today.month == date.month:
            print(relatie['voornaam'], relatie['achternaam'], "is jarig")
            update = {}
            update['contactDatum'] = convert_datetime_to_RFC3339(str(today))
            client.data_object.update(update, "Relatie", relatie['_additional']['id'])


def daily_activity_check(config, date=None):
    if date is None:
        now = datetime.datetime.now()
    else:
        now = date

    relaties = get_relaties(config)
    for relatie in relaties:
        _check_birthday(config, relatie, now)
        _check_anniversary(config, relatie, now)
        _check_contact(config, relatie, now)
