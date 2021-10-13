""" Check activity """

import datetime
import weaviate
from dateutil.parser import parse
from modules.query import get_relaties
from modules.sendmail import send_mail
from modules.utilities import get_weaviate_client
from modules.utilities import convert_datetime_to_RFC3339


def _select_bericht(schema, month):
    bericht = None
    if schema == 'A':
        if month <= 3:
            bericht = "./data/templates/A1_bericht.html"
        elif month <= 9:
            bericht = "./data/templates/A2_bericht.html"
        else:
            bericht = "./data/templates/A3_bericht.html"

    elif schema == 'B':
        if month <= 6:
            bericht = "./data/templates/B1_bericht.html"
        else:
            bericht = "./data/templates/B2_bericht.html"

    elif schema == 'C':
        bericht = "./data/templates/C1_bericht.html"

    return bericht


def _check_contact(config, relatie, today):

    action = None
    client = get_weaviate_client(config['weaviate'])
    contact = False

    if 'schema' in relatie and relatie['schema'] is not None:
        if relatie['schema'] == 'A':
            max_time = 90
        elif relatie['schema'] == 'B':
            max_time = 180
        elif relatie['schema'] == 'C':
            max_time = 360

    if 'contactDatum' in relatie and relatie['contactDatum'] is not None:
        date = parse(relatie['contactDatum'])
        naive = date.replace(tzinfo=None)
        if (today-naive).days > max_time:
            contact = True
    else:
        contact = True

    if contact:
        update = {}
        update['contactDatum'] = convert_datetime_to_RFC3339(str(today))
        client.data_object.update(update, "Relatie", relatie['_additional']['id'])
        bericht = _select_bericht(relatie['schema'], today.month)
        action = {}
        action['relatie'] = relatie
        action['type'] = "contact"
        action['subject'] = "Binnenkort afspraak voor bijpraten"
        action['template'] = bericht

    return action


def _check_anniversary(config, relatie, today):

    action = None
    anniversaries = [1, 5, 10, 15, 20, 25, 30]
    client = get_weaviate_client(config['weaviate'])
    if 'startDatum' in relatie and relatie['startDatum'] is not None:
        date = parse(relatie['startDatum'])
        if today.day == date.day and today.month == date.month:
            if today.year-date.year in anniversaries:
                action = {}
                action['relatie'] = relatie
                action['type'] = "anniversary"
                action['subject'] = "Relatie jubileum"
                action['template'] = "./data/templates/jubileumbericht.html"
                action['years'] = today.year-date.year


def _check_birthday(config, relatie, today):

    action = None
    client = get_weaviate_client(config['weaviate'])
    if 'geboorteDatum' in relatie and relatie['geboorteDatum'] is not None:
        date = parse(relatie['geboorteDatum'])
        if today.day == date.day and today.month == date.month:
            action = {}
            action['relatie'] = relatie
            action['subject'] = "Van harte proficiat met jouw verjaardag"
            action['type'] = "birthday"
            action['template'] = "./data/templates/verjaardagsbericht.html"

    return action


def daily_activity_check(config, date=None):
    if date is None:
        now = datetime.datetime.now()
    else:
        now = date

    relaties = get_relaties(config)
    for relatie in relaties:

        action = _check_birthday(config, relatie, now)
        if action is not None:
            send_mail(action)

        action = _check_anniversary(config, relatie, now)
        if action is not None:
            send_mail(action)

        action = _check_contact(config, relatie, now)
        if action is not None:
            send_mail(action)
