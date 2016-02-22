"""
File: boston_public_schools.py

Description: Boston Public Schools locations from data.cityofboston.gov
Author(s): Raaid Arshad and Michael Clawar

Notes:
"""

import datetime
import prov.model
import uuid

from mjclawar_rarshad.reference import mcra_structures as mcras
from mjclawar_rarshad.reference.mcra_structures import APIQuery, MCRASSettings, MCRASProvenance
from mjclawar_rarshad.reference.provenance import ProjectProvenance
from mjclawar_rarshad.tools.bdp_query import BDPQuery
from mjclawar_rarshad.tools.database_helpers import DatabaseHelper


class BostonPublicSchoolsSettings(MCRASSettings):
    @property
    def data_namespace(self):
        return mcras.BDP_NAMESPACE

    @property
    def data_entity(self):
        return 'boston_public_schools'

    @property
    def agent(self):
        return '%s:%s' % (self.data_namespace.name, self.data_entity)

    @property
    def base_url(self):
        return 'https://data.cityofboston.gov/resource/e29s-ympv.json'


class BostonPublicSchoolsProvenance(MCRASProvenance):
    def __init__(self, settings, database_helper, query=''):
        assert isinstance(settings, BostonPublicSchoolsSettings)
        assert isinstance(database_helper, DatabaseHelper)
        assert isinstance(query, str)
        self.settings = settings
        self.database_helper = database_helper
        self.query = query

    def update_provenance(self, start_time, end_time):
        """
        Writes a ProvDoc for the boston_public_schools.py script and saves to the collection

        Parameters
        ----------
        start_time: datetime.datetime
        end_time: datetime.datetime

        Returns
        -------
        """
        prov_obj = ProjectProvenance(database_helper=self.database_helper)
        prov_doc = prov_obj.prov_doc
        this_script = prov_doc.agent(self.settings.agent, mcras.PROVENANCE_PYTHON_SCRIPT)

        resource = prov_doc.entity('%s:%s' % (self.settings.data_namespace.name, self.settings.base_url))

        this_run = prov_doc.activity('%s:a%s' % (mcras.LOG_NAMESPACE.name, str(uuid.uuid4())), start_time, end_time,
                                     {prov.model.PROV_TYPE: mcras.PROVENANCE_ONT_RETRIEVAL,
                                      mcras.PROV_ONT_QUERY: '?' + self.query})

        prov_doc.wasAssociatedWith(this_run, this_script)
        prov_doc.used(this_run, resource, start_time)

        data_doc = prov_doc.entity('%s:%s' % (mcras.DAT_NAMESPACE.name, self.settings.data_entity),
                                   {prov.model.PROV_LABEL: 'Boston Public Schools',
                                    prov.model.PROV_TYPE: mcras.PROV_ONT_DATASET})

        prov_doc.wasAttributedTo(data_doc, this_script)
        prov_doc.wasGeneratedBy(data_doc, this_run, end_time)
        prov_doc.wasDerivedFrom(data_doc, resource, this_run, this_run, this_run)

        # TODO figure out with record
        prov_obj.write_provenance_json()
        # self.database_helper.record(prov_doc.serialize())


class BostonPublicSchoolsAPIQuery(APIQuery):
    def __init__(self, settings, database_helper, bdp_api):
        assert isinstance(settings, BostonPublicSchoolsSettings)
        assert isinstance(database_helper, DatabaseHelper)
        assert isinstance(bdp_api, BDPQuery)

        self.settings = settings
        self.database_helper = database_helper
        self.bdp_api = bdp_api

    def download_update_database(self):
        """
        Downloads data on Boston public schools, writes to a collection, and creates a provenance document

        Returns
        -------
        """
        start_time = datetime.datetime.now()
        data_json, api_query = self.bdp_api.api_query(base_url=self.settings.base_url,
                                                      select=['bldg_name', 'zipcode', 'sch_name',
                                                              'sch_type', 'location'])

        self.database_helper.insert_permanent_collection(self.settings.data_entity, data_json)

        end_time = datetime.datetime.now()

        boston_public_schools_provenance = BostonPublicSchoolsProvenance(self.settings, database_helper=self.database_helper, query=api_query)
        boston_public_schools_provenance.update_provenance(start_time=start_time, end_time=end_time)
