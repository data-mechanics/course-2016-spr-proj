"""
File: hospital_locations.py

Description: Hospital locations from data.cityofboston.gov
Author(s): Raaid Arshad and Michael Clawar

Notes:
"""

import datetime
import uuid

import prov.model

from mjclawar_rarshad.reference import mcra_structures as mcras
from mjclawar_rarshad.reference.mcra_structures import APIQuery, MCRASSettings, MCRASProvenance
from mjclawar_rarshad.tools.bdp_query import BDPQuery
from mjclawar_rarshad.tools.database_helpers import DatabaseHelper
from mjclawar_rarshad.tools.provenance import ProjectProvenance


class HospitalLocationsSettings(MCRASSettings):
    @property
    def data_namespace(self):
        return mcras.BDP_NAMESPACE

    @property
    def data_entity(self):
        return 'hospital_locations'

    @property
    def agent(self):
        return '%s:%s' % (mcras.ALG_NAMESPACE.name, self.data_entity)

    @property
    def base_url(self):
        return '46f7-2snz'


class HospitalLocationsProvenance(MCRASProvenance):
    def __init__(self, settings, database_helper, query=''):
        assert isinstance(settings, HospitalLocationsSettings)
        assert isinstance(database_helper, DatabaseHelper)
        assert isinstance(query, str)
        self.settings = settings
        self.database_helper = database_helper
        self.query = query

    def update_provenance(self, full_provenance=False, start_time=None, end_time=None):
        """
        Writes a ProvDoc for the hospital_locations.py script and saves to the collection

        Parameters
        ----------
        full_provenance: bool
        start_time: datetime.datetime
        end_time: datetime.datetime

        Returns
        -------
        """
        prov_obj = ProjectProvenance(database_helper=self.database_helper, full_provenance=full_provenance)
        prov_doc = prov_obj.prov_doc
        this_script = prov_doc.agent(self.settings.agent, mcras.PROVENANCE_PYTHON_SCRIPT)

        resource = prov_doc.entity('%s:%s' % (self.settings.data_namespace.name, self.settings.base_url))

        if full_provenance:
            this_run = prov_doc.activity('%s:a%s' % (mcras.LOG_NAMESPACE.name, str(uuid.uuid4())))
            prov_doc.used(this_run, resource)
        else:
            this_run = prov_doc.activity('%s:a%s' % (mcras.LOG_NAMESPACE.name, str(uuid.uuid4())), start_time, end_time,
                                         {prov.model.PROV_TYPE: mcras.PROVENANCE_ONT_RETRIEVAL,
                                          mcras.PROV_ONT_QUERY: '?' + self.query})

        prov_doc.wasAssociatedWith(this_run, this_script)

        data_doc = prov_doc.entity('%s:%s' % (mcras.DAT_NAMESPACE.name, self.settings.data_entity),
                                   {prov.model.PROV_LABEL: 'Hospital Locations',
                                    prov.model.PROV_TYPE: mcras.PROV_ONT_DATASET})

        prov_doc.wasAttributedTo(data_doc, this_script)

        if full_provenance:
            prov_doc.wasGeneratedBy(data_doc, this_run)
        else:
            prov_doc.wasGeneratedBy(data_doc, this_run, end_time)

        prov_doc.wasDerivedFrom(data_doc, resource, this_run)

        if full_provenance:
            prov_obj.write_provenance_json()
        else:
            self.database_helper.record(prov_doc.serialize())


class HospitalLocationsAPIQuery(APIQuery):
    def __init__(self, settings, database_helper, bdp_api):
        assert isinstance(settings, HospitalLocationsSettings)
        assert isinstance(database_helper, DatabaseHelper)
        assert isinstance(bdp_api, BDPQuery)

        self.settings = settings
        self.database_helper = database_helper
        self.bdp_api = bdp_api

    def download_update_database(self, full_provenance=False):
        """
        Downloads data on Boston hospital locations, writes to a collection, and creates a provenance document

        Parameters
        ----------
        full_provenance: bool
            Record the provenance for the entire project, or just for this class

        Returns
        -------
        """
        start_time = datetime.datetime.now()
        api_url = self.settings.data_namespace.link + self.settings.base_url + '.json'
        data_json, api_query = self.bdp_api.api_query(base_url=api_url,
                                                      select=['name', 'ad', 'zipcode',
                                                              'neigh', 'location'])

        self.database_helper.insert_permanent_collection(self.settings.data_entity, data_json)

        end_time = datetime.datetime.now()

        hospital_locations_provenance = \
            HospitalLocationsProvenance(self.settings, database_helper=self.database_helper, query=api_query)
        hospital_locations_provenance.\
            update_provenance(full_provenance=full_provenance, start_time=start_time, end_time=end_time)
