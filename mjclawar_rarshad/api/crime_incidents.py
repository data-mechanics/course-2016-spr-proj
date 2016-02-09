"""
Crime incidents API from data.cityofboston.gov

Michael Clawar and Raaid Arshad
"""

import urllib.request
import json
import pymongo
from mjclawar_rarshad.api_settings import api_token
from mjclawar_rarshad.structures import APIQuery

exec(open('../../pymongo_dm.py').read())


class CrimeAPIQuery(APIQuery):
    base_url = 'https://data.cityofboston.gov/resource/7cdf-6fgx.json?$$app_token=%s&' % api_token

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
        return json.dumps(r, sort_keys=True, indent=2)

    def download_update_database(self):
        s = self.api_query(limit=10, order='fromdate')
        print(s)
        # TODO write to database


if __name__ == '__main__':
    crime_api = CrimeAPIQuery()
    crime_api.download_update_database()
