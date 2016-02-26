"""
File: hospital_distances.py

Description: Calculates the distance between each property
Author(s): Raaid Arshad and Michael Clawar

Notes:
"""


import numpy as np
import pandas

import datetime
import prov
import uuid

from mjclawar_rarshad.reference import mcra_structures as mcras
from mjclawar_rarshad.reference.mcra_structures import MCRASProcessor, MCRASSettings, MCRASProvenance
from mjclawar_rarshad.reference.provenance import ProjectProvenance
from mjclawar_rarshad.tools.database_helpers import DatabaseHelper


def spherical_dist(pos1, pos2, r=3958.75):
    """
    Calculates the distance between two np arrays with [Longitude, Latitude]

    Modified from http://stackoverflow.com/a/19414306

    Parameters
    ----------
    pos1: np.array
    pos2: np.array
    r: float

    Returns
    -------
    np.array
    """

    pos1 *= np.pi / 180
    pos2 *= np.pi / 180

    cos_lat1 = np.cos(pos1[..., 0])
    cos_lat2 = np.cos(pos2[..., 0])
    cos_lat_d = np.cos(pos1[..., 0] - pos2[..., 0])
    cos_lon_d = np.cos(pos1[..., 1] - pos2[..., 1])
    dist = r * np.arccos(cos_lat_d - cos_lat1 * cos_lat2 * (1 - cos_lon_d))
    return dist


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
        return mcras.DAT_NAMESPACE.link + 'hospital_locations'


class HospitalDistancesProvenance(MCRASProvenance):
    def __init__(self, settings, database_helper):
        assert isinstance(settings, HospitalDistancesSettings)
        assert isinstance(database_helper, DatabaseHelper)
        self.settings = settings
        self.database_helper = database_helper

    def update_provenance(self, start_time, end_time):
        """
        Writes a ProvDoc for the hospital_distances.py script and saves to the collection

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

        this_run = prov_doc.activity('%s:a%s' % (mcras.LOG_NAMESPACE.name, str(uuid.uuid4())), start_time, end_time)

        prov_doc.wasAssociatedWith(this_run, this_script)
        prov_doc.used(this_run, resource, start_time)

        data_doc = prov_doc.entity('%s:%s' % (mcras.DAT_NAMESPACE.name, self.settings.data_entity),
                                   {prov.model.PROV_LABEL: 'Hospital Locations',
                                    prov.model.PROV_TYPE: mcras.PROV_ONT_DATASET})

        prov_doc.wasAttributedTo(data_doc, this_script)
        prov_doc.wasGeneratedBy(data_doc, this_run, end_time)
        prov_doc.wasDerivedFrom(data_doc, resource, this_run, this_run, this_run)

        # TODO figure out with record
        prov_obj.write_provenance_json()
        # self.database_helper.record(prov_doc.serialize())


class HospitalLocationsProcessor(MCRASProcessor):
    def __init__(self, settings, database_helper):
        assert isinstance(settings, HospitalDistancesSettings)
        assert isinstance(database_helper, DatabaseHelper)
        self.settings = settings
        self.database_helper = database_helper

    def run_processor(self):
        start_time = datetime.datetime.now()
        df_prop = self._load_prep_dfprop()
        df_hosp = self._load_prep_dfhosp()
        df_prop = self._calc_distances(df_prop, df_hosp)

        self.database_helper.insert_permanent_pandas(self.settings.data_entity, df_prop)

        end_time = datetime.datetime.now()

        hospital_distances_provenance = HospitalDistancesProvenance(self.settings, database_helper=self.database_helper)
        hospital_distances_provenance.update_provenance(start_time=start_time, end_time=end_time)

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
        df_prop = df_prop.merge(df_prop['location'].apply(lambda x: pandas.Series({'LATITUDE': float(x.split(',')[0][1:]),
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
        pos_prop = np.array(df_prop['LATITUDE'], df_prop['LONGITUDE'])
        pos_hosp = np.array(df_hosp['LATITUDE'], df_hosp['LONGITUDE'])

        (prop_rows, prop_columns) = pos_prop.shape
        (hosp_rows, hosp_columns) = pos_hosp.shape

        distances = np.zeros((hosp_rows,prop_rows))

        for k in range(hosp_rows):
            distances = spherical_dist(pos_prop.copy(), pos_hosp[k].copy())

        minindices = np.argmin(distances, 0)
        mindistances = np.amin(distances, 0)

        hosp_names = df_hosp['name']
        df_prop['NEAREST_HOSPITAL'] = hosp_names.iloc[minindices]
        df_prop['MIN_DISTANCE'] = mindistances
        df_prop = df_prop[['LONGITUDE', 'LATITUDE', 'MIN_DISTANCE']].copy()
        return df_prop
