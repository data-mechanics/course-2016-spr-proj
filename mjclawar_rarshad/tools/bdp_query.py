"""
File: bdp_query.py

Description: City of Boston API query functions
Author(s): Raaid Arshad and Michael Clawar

Notes:
"""

import json
import urllib.request


class BDPQuery:
    def __init__(self, api_token):
        self.api_token = api_token

    def api_query(self, base_url, limit=None, order=None, select=None, where=None):
        """
        Creates and sends the API query according to the Socrata API standards: https://dev.socrata.com/docs/queries/

        Parameters
        ----------
        base_url: str
            The url of the full data set, modified by the other parameters
        limit: int
            Number of observations to return ($limit in SoQL)
        order: str
            Name of the variable to order by ($order in SoQL)
        select: list
            List of the variables to return. Returns all in data set if None ($select in SoQL)
        where: str
            Where condition ($where in SoQL)

        Returns
        -------
        json, str
        """
        query_url = self.get_query_url(base_url, limit, order, select, where)
        print('Getting query from', query_url)
        response = urllib.request.urlopen(query_url).read().decode('utf-8')
        r = json.loads(response)
        print('Done getting query')
        return r, query_url.split('?')[1]

    def get_query_url(self, base_url, limit, order, select, where):
        """
        Creates the query url to use from the parameters

        Parameters
        ----------
        base_url: str
            The url of the full data set, modified by the other parameters
        limit: int
            Number of observations to return ($limit in SoQL)
        order: str
            Name of the variable to order by ($order in SoQL)
        select: list
            List of the variables to return. Returns all in data set if None ($select in SoQL)
        where: str
            Where condition ($where in SoQL)

        Returns
        -------
        str
        """
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
                    query_url += ',' + select_column

        if where is not None:
            assert isinstance(where, str)
            query_url += '&$where=' + where.replace(' ', '%20')

        return query_url
