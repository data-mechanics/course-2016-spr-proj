"""
Crime incidents API from data.cityofboston.gov

Michael Clawar and Raaid Arshad
"""

import urllib.request
import json
import pymongo
from mjclawar_rarshad.reference import api_token, provenance_document, provenance_file
from mjclawar_rarshad.structures import APIQuery

exec(open('../../pymongo_dm.py').read())


class CrimeAPIQuery(APIQuery):
    base_url = 'https://data.cityofboston.gov/resource/7cdf-6fgx.json?$$app_token=%s&' % api_token

    @property
    def data_namespace(self):
        return 'cityofboston'

    @property
    def data_entity(self):
        return 'crime_data'

    def __init__(self, agent):
        self.agent = agent
        if self.data_namespace not in provenance_document._namespaces:
            provenance_document.add_namespace(self.data_namespace, self.base_url)

    @staticmethod
    def api_query(limit=100, order=None, select=None, where=None):
        assert isinstance(limit, int)
        query_url = CrimeAPIQuery.base_url + '$limit=%s' %limit
        if order is not None:
            assert isinstance(order, str)
            query_url += '&$order=%s' % order + '%20DESC'
        if select is not None:
            assert isinstance(select, list)
        if where is not None:
            assert isinstance(where, str)

        response = urllib.request.urlopen(query_url).read().decode('utf-8')
        r = json.loads(response)
        return query_url, json.dumps(r, sort_keys=True, indent=2)

    def download_update_database(self):
        query_url, s = self.api_query(limit=10, order='fromdate')
        agent = provenance_document.agent('people:%s' % self.agent)
        entity = provenance_document.entity('%s:%s' % (self.data_namespace, query_url))
        provenance_document.wasAttributedTo(entity, agent)
        print(provenance_document.serialize())
        provenance_document.serialize(provenance_file, indent=2)
        # TODO write to database


if __name__ == '__main__':
    crime_api = CrimeAPIQuery('mjclawar')
    crime_api.download_update_database()
