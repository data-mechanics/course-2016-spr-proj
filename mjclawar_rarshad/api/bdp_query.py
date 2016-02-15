"""
CityOfBoston.gov API query functions

Michael Clawar and Raaid Arshad
"""
import json
import urllib.request


class BDPQuery:
    def __init__(self, api_token):
        self.api_token = api_token

    def api_query(self, base_url, limit=None, order=None, select=None, where=None):
        query_url = self.get_query_url(base_url, limit, order, select, where)
        response = urllib.request.urlopen(query_url).read().decode('utf-8')
        r = json.loads(response)
        return r, query_url.split('?')[1]

    def get_query_url(self, base_url, limit, order, select, where):
        # TODO make me in auth.json
        query_url = base_url + '?$$app_token=%s&' % self.api_token

        if limit is not None:
            query_url += '$limit=%s' % limit

        if order is not None:
            assert isinstance(order, str)
            query_url += '&$order=%s' % order + '%20DESC'

        if select is not None:
            assert isinstance(select, list)
            assert all([isinstance(x, str) for x in select])
            query_url += '&$select=%s' % select[0]
            if len(select) > 1:
                for select_column in select[1:]:
                    query_url += ',%20' + select_column

        if where is not None:
            assert isinstance(where, str)
            query_url += '&$where=' + where.replace(' ', '%20')

        return query_url
