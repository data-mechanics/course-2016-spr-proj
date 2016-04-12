"""
File: school_distances.py

Description: Calculates the distance between each property and the nearest school and creates a
permanent collection with the name and distance of the nearest school
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


class SchoolDistancesSettings(MCRASSettings):
    @property
    def data_namespace(self):
        return mcras.DAT_NAMESPACE

    @property
    def data_entity(self):
        return 'school_distances'

    @property
    def agent(self):
        return '%s:%s' % (mcras.ALG_NAMESPACE.name, self.data_entity)

    @property
    def base_url(self):
        return 'school_locations'

    @property
    def resource_properties(self):
        return 'property_assessment'


class SchoolDistancesProvenance(MCRASProvenance):
    def __init__(self, settings, database_helper):
        assert isinstance(settings, SchoolDistancesSettings)
        assert isinstance(database_helper, DatabaseHelper)
        self.settings = settings
        self.database_helper = database_helper

    def update_provenance(self, full_provenance=False, start_time=None, end_time=None):
        """
        Writes a ProvDoc for the school_distances.py script and saves to the collection

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

        resource_school = \
            prov_doc.entity('%s:%s' % (self.settings.data_namespace.name, self.settings.base_url))
        resource_properties = \
            prov_doc.entity('%s:%s' % (self.settings.data_namespace.name, self.settings.resource_properties))

        if full_provenance:
            this_run = prov_doc.activity('%s:a%s' % (mcras.LOG_NAMESPACE.name, str(uuid.uuid4())))
            prov_doc.used(this_run, resource_school)
            prov_doc.used(this_run, resource_properties)
        else:
            this_run = prov_doc.activity('%s:a%s' % (mcras.LOG_NAMESPACE.name, str(uuid.uuid4())), start_time, end_time,
                                         {prov.model.PROV_TYPE: mcras.PROV_ONT_EXTENSION})

        prov_doc.wasAssociatedWith(this_run, this_script)

        data_doc = prov_doc.entity('%s:%s' % (mcras.DAT_NAMESPACE.name, self.settings.data_entity),
                                   {prov.model.PROV_LABEL: 'School Locations',
                                    prov.model.PROV_TYPE: mcras.PROV_ONT_DATASET})

        prov_doc.wasAttributedTo(data_doc, this_script)
        if full_provenance:
            prov_doc.wasGeneratedBy(data_doc, this_run)
        else:
            prov_doc.wasGeneratedBy(data_doc, this_run, end_time)

        prov_doc.wasDerivedFrom(data_doc, resource_school, this_run)
        prov_doc.wasDerivedFrom(data_doc, resource_properties, this_run)

        if full_provenance:
            prov_obj.write_provenance_json()
        else:
            self.database_helper.record(prov_doc.serialize())


class SchoolLocationsProcessor(MCRASProcessor):
    def __init__(self, settings, database_helper):
        assert isinstance(settings, SchoolDistancesSettings)
        assert isinstance(database_helper, DatabaseHelper)
        self.settings = settings
        self.database_helper = database_helper

    def run_processor(self, full_provenance=False):
        start_time = datetime.datetime.now()
        df_prop = self._load_prep_df_prop()
        df_school = self._load_prep_df_school()
        df_prop = self._calc_distances(df_prop, df_school)

        self.database_helper.insert_permanent_pandas(self.settings.data_entity, df_prop)

        end_time = datetime.datetime.now()

        school_distances_provenance = SchoolDistancesProvenance(self.settings, database_helper=self.database_helper)
        school_distances_provenance.update_provenance(full_provenance=full_provenance, start_time=start_time,
                                                      end_time=end_time)

    def _load_prep_df_school(self):
        """
        Reads the pandas.DataFrame from the MongoDB collection, cleans it up, and returns it

        Returns
        -------
        pandas.DataFrame
        """
        df_school = self.database_helper.load_permanent_pandas('school_locations', cols=['sch_name', 'location'])
        df_school = df_school.merge(df_school['location'].apply(lambda x: pandas.Series({'NEEDS_RECODE': x['needs_recoding'],
                                                                                   'LONGITUDE': float(x['longitude']),
                                                                                   'LATITUDE': float(x['latitude'])})),
                                left_index=True, right_index=True)
        df_school = df_school[(df_school['LONGITUDE'] != 0) & (df_school['LATITUDE'] != 0)].copy()

        return df_school

    def _load_prep_df_prop(self):
        """
        Reads the pandas.DataFrame from the MongoDB collection, cleans it up, and returns it

        Returns
        -------
        pandas.DataFrame
        """

        df_prop = self.database_helper.load_permanent_pandas(
            'property_assessment', cols=['location', 'av_total', 'living_area']
        )
        df_list = []
        for i in range(len(df_prop)):
            df_row = df_prop.iloc[i, :]
            try:
                lat = float(df_row['location'].split(',')[0][1:])
                long = float(df_row['location'].split(',')[1][0:-1])
                av_total = float(df_row['av_total'])
                living_area = float(df_row['living_area'])
                df_list.append((lat, long, av_total, living_area))
            except:
                pass

        df_prop = pandas.DataFrame(df_list)
        assert isinstance(df_prop, pandas.DataFrame)
        df_prop.columns = ['LATITUDE', 'LONGITUDE', 'AV_TOTAL', 'LIVING_AREA']

        df_prop = df_prop[(df_prop['LONGITUDE'] != 0) & (df_prop['LATITUDE'] != 0)].copy()

        return df_prop

    @staticmethod
    def _calc_distances(df_prop, df_school):
        """
       calculates the distance from each property to each school

        Parameters
        ----------
        df_prop: pandas.DataFrame
        df_school: pandas.DataFrame

        Returns
        -------
        pandas.DataFrame
        """
        pos_prop = df_prop[['LATITUDE', 'LONGITUDE']].values
        pos_school = df_school[['LATITUDE', 'LONGITUDE']].values

        (prop_rows, prop_columns) = pos_prop.shape
        (school_rows, school_columns) = pos_school.shape

        distances = np.zeros((school_rows, prop_rows))

        for k in range(school_rows):
            distances[k, :] = distance_estimators.spherical_distance(pos_prop.copy(), pos_school[k].copy())

        min_indices = np.argmin(distances, 0)
        min_distances = np.amin(distances, 0)

        school_names = df_school['sch_name'].values
        df_prop['NEAREST_SCHOOL'] = school_names[min_indices]
        df_prop['MIN_DISTANCE'] = min_distances
        df_prop = df_prop[['LONGITUDE', 'LATITUDE', 'NEAREST_SCHOOL', 'MIN_DISTANCE', 'AV_TOTAL',
                           'LIVING_AREA']].copy()
        return df_prop
