"""
Crime incidents API from data.cityofboston.gov

Michael Clawar and Raaid Arshad
"""

import datetime
import uuid

import prov.model


from mjclawar_rarshad import mcra_structures as mcras
from mjclawar_rarshad.database_helpers import DatabaseHelper
from mjclawar_rarshad.api.bdp_query import BDPQuery
from mjclawar_rarshad.mcra_structures import APIQuery, MCRASSettings, MCRASProvenance
from mjclawar_rarshad.setup_provenance import ProjectProvenance


class CrimeSettings(MCRASSettings):
    @property
    def data_namespace(self):
        return mcras.BDP_NAMESPACE

    @property
    def data_entity(self):
        return 'crime_incidents'

    @property
    def agent(self):
        return '%s:crime_incidents' % self.data_namespace.name

    @property
    def base_url(self):
        return 'https://data.cityofboston.gov/resource/7cdf-6fgx.json'


class CrimeProvenance(MCRASProvenance):
    def __init__(self, settings, prov_doc, query=''):
        assert isinstance(settings, CrimeSettings)
        assert isinstance(prov_doc, ProjectProvenance)
        assert isinstance(query, str)
        self.settings = settings
        self.query = query
        self.prov_doc = prov_doc.prov_doc

    def update_provenance(self, start_time, end_time):
        this_script = self.prov_doc.agent(self.settings.agent, mcras.PROVENANCE_PYTHON_SCRIPT)

        resource = self.prov_doc.entity('%s:%s' % (self.settings.data_namespace.name,
                                              self.settings.base_url))

        this_run = self.prov_doc.activity('%s:a%s' % (mcras.LOG_NAMESPACE.name, str(uuid.uuid4())), start_time, end_time,
                                     {prov.model.PROV_TYPE: mcras.PROVENANCE_ONT_RETRIEVAL,
                                      mcras.PROV_ONT_QUERY: '?' + self.query})

        self.prov_doc.wasAssociatedWith(this_run, this_script)
        self.prov_doc.used(this_run, resource, start_time)

        data_doc = self.prov_doc.entity('%s:%s' % (mcras.DAT_NAMESPACE.name, self.settings.data_entity),
                                   {prov.model.PROV_LABEL: 'Crimes Committed', prov.model.PROV_TYPE:
                                       mcras.PROV_ONT_DATASET})

        self.prov_doc.wasAttributedTo(data_doc, this_script)
        self.prov_doc.wasGeneratedBy(data_doc, this_run, end_time)
        self.prov_doc.wasDerivedFrom(data_doc, resource, this_run, this_run, this_run)


class CrimeAPIQuery(APIQuery):
    def __init__(self, settings, database_helper, bdp_api, project_provenance):
        assert isinstance(settings, CrimeSettings)
        assert isinstance(database_helper, DatabaseHelper)
        assert isinstance(bdp_api, BDPQuery)
        assert isinstance(project_provenance, ProjectProvenance)

        self.settings = settings
        self.database_helper = database_helper
        self.bdp_api = bdp_api
        self.project_provenance = project_provenance

    def download_update_database(self):
        start_time = datetime.datetime.now()
        data_json, api_query = self.bdp_api.api_query(base_url=self.settings.base_url, limit=10, order='fromdate')

        self.database_helper.insert_permanent_db(self.settings.data_entity, data_json)

        end_time = datetime.datetime.now()

        crime_provenance = CrimeProvenance(self.settings, prov_doc=self.project_provenance, query=api_query)
        crime_provenance.update_provenance(start_time=start_time, end_time=end_time)
