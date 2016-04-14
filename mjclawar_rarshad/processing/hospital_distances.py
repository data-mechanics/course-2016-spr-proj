"""
File: hospital_distances.py

Description: Calculates the distance between each property and the nearest hospital and creates a
permanent collection with the name and distance of the nearest hospital
Author(s): Raaid Arshad and Michael Clawar

Notes:
"""

import datetime
import uuid

import numpy as np
import pandas
import prov

from mjclawar_rarshad.reference import mcra_structures as mcras
from mjclawar_rarshad.reference.mcra_structures import MCRASProcessor, MCRASSettings, MCRASProvenance
from mjclawar_rarshad.tools.database_helpers import DatabaseHelper
from mjclawar_rarshad.tools.provenance import ProjectProvenance
from mjclawar_rarshad.tools import distance_estimators


class HospitalDistancesSettings(MCRASSettings):
    @property
    def data_namespace(self):
        return mcras.DAT_NAMESPACE

    @property
    def data_entity(self):
        return 'hospital_distances'

    @property
    def agent(self):
        return '%s:%s' % (mcras.ALG_NAMESPACE.name, self.data_entity)

    @property
    def base_url(self):
        return 'hospital_locations'

    @property
    def resource_properties(self):
        return 'property_assessment'


class HospitalDistancesProvenance(MCRASProvenance):
    def __init__(self, settings, database_helper):
        assert isinstance(settings, HospitalDistancesSettings)
        assert isinstance(database_helper, DatabaseHelper)
        self.settings = settings
        self.database_helper = database_helper

    def update_provenance(self, full_provenance=False, start_time=None, end_time=None):
        """
        Writes a ProvDoc for the hospital_distances.py script and saves to the collection

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

        resource_hospitals = \
            prov_doc.entity('%s:%s' % (self.settings.data_namespace.name, self.settings.base_url))
        resource_properties = \
            prov_doc.entity('%s:%s' % (self.settings.data_namespace.name, self.settings.resource_properties))

        if full_provenance:
            this_run = prov_doc.activity('%s:a%s' % (mcras.LOG_NAMESPACE.name, str(uuid.uuid4())))
            prov_doc.used(this_run, resource_hospitals)
            prov_doc.used(this_run, resource_properties)
        else:
            this_run = prov_doc.activity('%s:a%s' % (mcras.LOG_NAMESPACE.name, str(uuid.uuid4())), start_time, end_time,
                                         {prov.model.PROV_TYPE: mcras.PROV_ONT_EXTENSION})

        prov_doc.wasAssociatedWith(this_run, this_script)

        data_doc = prov_doc.entity('%s:%s' % (mcras.DAT_NAMESPACE.name, self.settings.data_entity),
                                   {prov.model.PROV_LABEL: 'Hospital Locations',
                                    prov.model.PROV_TYPE: mcras.PROV_ONT_DATASET})

        prov_doc.wasAttributedTo(data_doc, this_script)
        if full_provenance:
            prov_doc.wasGeneratedBy(data_doc, this_run)
        else:
            prov_doc.wasGeneratedBy(data_doc, this_run, end_time)

        prov_doc.wasDerivedFrom(data_doc, resource_hospitals, this_run)
        prov_doc.wasDerivedFrom(data_doc, resource_properties, this_run)

        if full_provenance:
            prov_obj.write_provenance_json()
        else:
            self.database_helper.record(prov_doc.serialize())


class HospitalLocationsProcessor(MCRASProcessor):
    def __init__(self, settings, database_helper):
        assert isinstance(settings, HospitalDistancesSettings)
        assert isinstance(database_helper, DatabaseHelper)
        self.settings = settings
        self.database_helper = database_helper

    def run_processor(self, full_provenance=False):
        start_time = datetime.datetime.now()
        df_prop = self._load_prep_df_prop()
        df_hosp = self._load_prep_df_hosp()
        df_prop = self._calc_distances(df_prop, df_hosp)

        self.database_helper.insert_permanent_pandas(self.settings.data_entity, df_prop)

        end_time = datetime.datetime.now()

        hospital_distances_provenance = HospitalDistancesProvenance(self.settings, database_helper=self.database_helper)
        hospital_distances_provenance.update_provenance(full_provenance=full_provenance, start_time=start_time,
                                                        end_time=end_time)

    def _load_prep_df_hosp(self):
        """
        Reads the pandas.DataFrame from the MongoDB collection, cleans it up, and returns it

        Returns
        -------
        pandas.DataFrame
        """
        df_hosp = self.database_helper.load_permanent_pandas('hospital_locations', cols=['name', 'location'])
        df_hosp = df_hosp.merge(df_hosp['location'].apply(lambda x: pandas.Series({'NEEDS_RECODE': x['needs_recoding'],
                                                                                   'LONGITUDE': float(x['longitude']),
                                                                                   'LATITUDE': float(x['latitude'])})),
                                left_index=True, right_index=True)
        df_hosp = df_hosp[(df_hosp['LONGITUDE'] != 0) & (df_hosp['LATITUDE'] != 0)].copy()

        return df_hosp

    def _load_prep_df_prop(self):
        """
        Reads the pandas.DataFrame from the MongoDB collection, cleans it up, and returns it

        Returns
        -------
        pandas.DataFrame
        """
        df_prop = self.database_helper.load_permanent_pandas('property_assessment', cols=['location'])
        df_prop = df_prop.merge(df_prop['location'].
                                apply(lambda x: pandas.Series({'LATITUDE': float(x.split(',')[0][1:]),
                                                               'LONGITUDE': float(x.split(',')[1][0:-1])})),
                                left_index=True, right_index=True)
        df_prop = df_prop[(df_prop['LONGITUDE'] != 0) & (df_prop['LATITUDE'] != 0)].copy()

        return df_prop

    @staticmethod
    def _calc_distances(df_prop, df_hosp):
        """
       calculates the distance from each property to each hospital

        Parameters
        ----------
        df_prop: pandas.DataFrame
        df_hosp: pandas.DataFrame

        Returns
        -------
        pandas.DataFrame
        """
        pos_prop = df_prop[['LATITUDE', 'LONGITUDE']].values
        pos_hosp = df_hosp[['LATITUDE', 'LONGITUDE']].values

        (prop_rows, prop_columns) = pos_prop.shape
        (hosp_rows, hosp_columns) = pos_hosp.shape

        distances = np.zeros((hosp_rows, prop_rows))

        for k in range(hosp_rows):
            distances[k, :] = distance_estimators.spherical_distance(pos_prop.copy(), pos_hosp[k].copy())

        min_indices = np.argmin(distances, 0)
        min_distances = np.amin(distances, 0)

        hosp_names = df_hosp['name'].values
        df_prop['NEAREST_HOSPITAL'] = hosp_names[min_indices]
        df_prop['MIN_DISTANCE'] = min_distances
        df_prop = df_prop[['LONGITUDE', 'LATITUDE', 'NEAREST_HOSPITAL', 'MIN_DISTANCE']].copy()
        return df_prop
