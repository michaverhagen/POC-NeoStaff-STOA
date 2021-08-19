""" This module queries Weaviate """

import weaviate
from weaviate.connect import REST_METHOD_POST

QUERY_TEMPLATE = """
{
  Get {
    Relatie {
      _additional {
          id
      }
      achternaam
      email
      geboorteDatum
      contactDatum
      identifier
      mobielNummer
      startDatum
      tussenvoegsel
      voornaam
    }
  }
}
"""


def get_relaties(config):

    relaties = []

    connection = weaviate.connect.Connection(config['weaviate']['url'])
    query = {"query": QUERY_TEMPLATE}
    result = connection.run_rest("/graphql", REST_METHOD_POST, query)
    outcome = result.json()
    if 'errors' not in outcome:
        for relatie in outcome['data']['Get']['Relatie']:
            relaties.append(relatie)

    return relaties
