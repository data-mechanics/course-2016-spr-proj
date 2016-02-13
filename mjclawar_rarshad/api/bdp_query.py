"""
CityOfBoston.gov API query functions

Michael Clawar and Raaid Arshad
"""
import json
import urllib.request


class BDPQuery:
    def __init__(self, api_token):
        self.api_token = api_token

    def api_query(self, base_url, limit=100, order=None, select=None, where=None):
        query_url = self.get_query_url(base_url, limit, order, select, where)
        response = urllib.request.urlopen(query_url).read().decode('utf-8')
        r = json.loads(response)
        return r, query_url.split('?')[1]

    def get_query_url(self, base_url, limit, order, select, where):
        # TODO make me in auth.json
        query_url = base_url + '?$$app_token=%s&' % self.api_token + '$limit=%s' % limit

        if order is not None:
            assert isinstance(order, str)
            query_url += '&$order=%s' % order + '%20DESC'

        if select is not None:
            assert isinstance(select, list)

        if where is not None:
            assert isinstance(where, str)

        return query_url
