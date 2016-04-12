"""
File: home_value_model.py

Description: Using k-nearest neighbors regression to map home prices across Boston
Author(s): Raaid Arshad and Michael Clawar

Notes:
"""

import datetime
import numpy as np
import pandas
import prov
import uuid

from mjclawar_rarshad.reference import mcra_structures as mcras
from mjclawar_rarshad.reference.mcra_structures import MCRASProcessor, MCRASSettings, MCRASProvenance
from mjclawar_rarshad.tools.database_helpers import DatabaseHelper
from mjclawar_rarshad.tools.provenance import ProjectProvenance
from mjclawar_rarshad.tools.mcras_plotting import MCRASPlotting

from sklearn import neighbors
from matplotlib.colors import ListedColormap
import matplotlib.pyplot as plt
import mplleaflet


class HomeValueModelSettings(MCRASSettings):
    @property
    def data_namespace(self):
        return mcras.DAT_NAMESPACE

    @property
    def data_entity(self):
        return 'home_value_model'

    @property
    def agent(self):
        return '%s:%s' % (mcras.ALG_NAMESPACE.name, self.data_entity)

    @property
    def base_url(self):
        return 'home_value_model'

    @property
    def resource_properties(self):
        return 'property_assessment'


class HomeValueModelProvenance(MCRASProvenance):
    def __init__(self, settings, database_helper):
        assert isinstance(settings, HomeValueModelSettings)
        assert isinstance(database_helper, DatabaseHelper)
        self.settings = settings
        self.database_helper = database_helper

    def update_provenance(self, full_provenance=False, start_time=None, end_time=None):
        """
        Writes a ProvDoc for the crime_knn.py script and saves to the collection

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

        resource_properties = \
            prov_doc.entity('%s:%s' % (self.settings.data_namespace.name, self.settings.resource_properties))

        if full_provenance:
            this_run = prov_doc.activity('%s:a%s' % (mcras.LOG_NAMESPACE.name, str(uuid.uuid4())))
            prov_doc.used(this_run, resource_properties)
        else:
            this_run = prov_doc.activity('%s:a%s' % (mcras.LOG_NAMESPACE.name, str(uuid.uuid4())), start_time, end_time,
                                         {prov.model.PROV_TYPE: mcras.PROV_ONT_EXTENSION})

        prov_doc.wasAssociatedWith(this_run, this_script)

        data_doc = prov_doc.entity('%s:%s' % (mcras.DAT_NAMESPACE.name, self.settings.data_entity),
                                   {prov.model.PROV_LABEL: 'Home Value Model',
                                    prov.model.PROV_TYPE: mcras.PROV_ONT_DATASET})

        prov_doc.wasAttributedTo(data_doc, this_script)
        if full_provenance:
            prov_doc.wasGeneratedBy(data_doc, this_run)
        else:
            prov_doc.wasGeneratedBy(data_doc, this_run, end_time)

        prov_doc.wasDerivedFrom(data_doc, resource_properties, this_run)

        if full_provenance:
            prov_obj.write_provenance_json()
        else:
            self.database_helper.record(prov_doc.serialize())


class HomeValueModelProcessor(MCRASProcessor):
    def __init__(self, settings, database_helper):
        assert isinstance(settings, HomeValueModelSettings)
        assert isinstance(database_helper, DatabaseHelper)
        self.settings = settings
        self.database_helper = database_helper

    def run_processor(self, full_provenance=False):
        start_time = datetime.datetime.now()
        df = self._load_prep_df()

        # Boundaries for subset of Boston
        x_min, x_max = -71.0725, -71.05
        y_min, y_max = 42.35, 42.368
        bounds = ((x_min, y_min), (x_max, y_max))

        df = df[(df['LONGITUDE'] < x_max) & (df['LONGITUDE'] > x_min) &
                (df['LATITUDE'] < y_max) & (df['LATITUDE'] > y_min)].copy()

        self._knn_property_values(df, bounds)

        # TODO write to database
        # self.database_helper.insert_permanent_collection(self.settings.data_entity, html_json)

        end_time = datetime.datetime.now()

        home_value_provenance = \
            HomeValueModelProvenance(self.settings, database_helper=self.database_helper)
        home_value_provenance.\
            update_provenance(full_provenance=full_provenance, start_time=start_time, end_time=end_time)

    def _load_prep_df(self):
        df = self.database_helper.load_permanent_pandas(
            'property_assessment',
            cols=['location', 'av_total', 'living_area']
        )

        df_list = []
        for i in range(len(df)):
            df_row = df.iloc[i, :]
            try:
                lat = float(df_row['location'].split(',')[0][1:])
                long = float(df_row['location'].split(',')[1][0:-1])
                av_total = float(df_row['av_total'])
                living_area = float(df_row['living_area'])
                df_list.append((lat, long, av_total, living_area))
            except:
                pass

        df = pandas.DataFrame(df_list)
        assert isinstance(df, pandas.DataFrame)

        df.columns = ['LATITUDE', 'LONGITUDE', 'AV_TOTAL', 'LIVING_AREA']
        df = df[df['LATITUDE'] != 0]
        df = df[df['LONGITUDE'] != 0]
        df = df[df['AV_TOTAL'] != 0]
        df = df[df['LIVING_AREA'] != 0]
        df['PPSQFT'] = df['AV_TOTAL'] / df['LIVING_AREA']
        df = df[(df['PPSQFT'] < 500) & (df['PPSQFT'] >= 50)]

        return df

    @staticmethod
    def _knn_property_values(df, bounds):
        clf = neighbors.KNeighborsRegressor(n_neighbors=5, weights='distance')
        X = df[['LATITUDE', 'LONGITUDE']].values
        y = df['PPSQFT']

        clf.fit(X, y)

        h = .0005  # step size in the mesh

        # Plot the decision boundary. For that, we will assign a color to each
        # point in the mesh [x_min, m_max]x[y_min, y_max].
        yy, xx = np.meshgrid(np.arange(bounds[0][0], bounds[1][0], h),
                             np.arange(bounds[0][1], bounds[1][1], h))
        clf.fit(X, y)
        Z = clf.predict(np.c_[xx.ravel(), yy.ravel()])

        # Put the result into a color plot
        Z = Z.reshape(xx.shape)
        MCRASPlotting.leaflet_heatmap(yy=yy, xx=xx, Z=Z, bounds=bounds, map_path='home_value_model.html',
                                      legend_text='Estimated value per square foot of home ($)')
