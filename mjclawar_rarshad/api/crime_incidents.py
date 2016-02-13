"""
Crime incidents API from data.cityofboston.gov

Michael Clawar and Raaid Arshad
"""

import datetime

from mjclawar_rarshad import mcra_structures as mcras
from mjclawar_rarshad.reference import provenance_document
from mjclawar_rarshad.mcra_structures import APIQuery
from mjclawar_rarshad.api import bdp_api

exec(open('../../pymongo_dm.py').read())


class CrimeAPIQuery(APIQuery):
    base_url = 'https://data.cityofboston.gov/resource/7cdf-6fgx.json?'

    @property
    def data_namespace(self):
        return mcras.BDP_NAMESPACE

    @property
    def data_entity(self):
        return 'crime_data'

    def __init__(self, agent):
        self.agent = agent
        if self.data_namespace not in provenance_document._namespaces:
            provenance_document.add_namespace(self.data_namespace, self.base_url)

    def download_update_database(self):
        start_time = datetime.datetime.now()
        data_json = bdp_api.api_query(base_url=self.base_url, limit=10, order='fromdate')



        # TODO write to database


if __name__ == '__main__':
    crime_api = CrimeAPIQuery('mjclawar')
    crime_api.download_update_database()
