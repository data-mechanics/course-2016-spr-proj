"""
File: crime_centroids.py

Description: Calculates centroids for crime committed in Boston using k-means
Author(s): Raaid Arshad and Michael Clawar

Notes:
"""

import datetime
import uuid

import numpy as np
import pandas
import prov
from sklearn import cluster

from mjclawar_rarshad.reference import mcra_structures as mcras
from mjclawar_rarshad.reference.mcra_structures import MCRASProcessor, MCRASSettings, MCRASProvenance
from mjclawar_rarshad.tools.database_helpers import DatabaseHelper
from mjclawar_rarshad.tools.provenance import ProjectProvenance
from mjclawar_rarshad.tools import distance_estimators


def sse_group(df, columns):
    """
    Calculates the SSE for a cluster given a dataframe with a list of X columns

    Parameters
    ----------
    df: pandas.DataFrame
    columns: list

    Returns
    -------
    np.float
    """
    distances = distance_estimators.spherical_distance(df[columns].values, np.array([df[columns].mean().values]))
    distances *= distances
    sse = sum(distances)
    return sse


class CrimeCentroidsSettings(MCRASSettings):
    @property
    def data_namespace(self):
        return mcras.DAT_NAMESPACE

    @property
    def data_entity(self):
        return 'crime_centroids'

    @property
    def agent(self):
        return '%s:%s' % (mcras.ALG_NAMESPACE.name, self.data_entity)

    @property
    def base_url(self):
        return 'crime_incidents'


class CrimeCentroidsProvenance(MCRASProvenance):
    def __init__(self, settings, database_helper):
        assert isinstance(settings, CrimeCentroidsSettings)
        assert isinstance(database_helper, DatabaseHelper)
        self.settings = settings
        self.database_helper = database_helper

    def update_provenance(self, full_provenance=False, start_time=None, end_time=None):
        """
        Writes a ProvDoc for the crime_centroids.py script and saves to the collection

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
                                         {prov.model.PROV_TYPE: mcras.PROV_ONT_EXTENSION})

        prov_doc.wasAssociatedWith(this_run, this_script)

        data_doc = prov_doc.entity('%s:%s' % (mcras.DAT_NAMESPACE.name, self.settings.data_entity),
                                   {prov.model.PROV_LABEL: 'Crime Centroids',
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


class CrimeCentroidsProcessor(MCRASProcessor):
    def __init__(self, settings, database_helper):
        assert isinstance(settings, CrimeCentroidsSettings)
        assert isinstance(database_helper, DatabaseHelper)
        self.settings = settings
        self.database_helper = database_helper

    def run_processor(self, full_provenance=False):
        start_time = datetime.datetime.now()
        df = self._load_prep_df()
        df_groups = self._kmeans_fit_predict(df, n_groups=15)

        self.database_helper.insert_permanent_pandas(self.settings.data_entity, df_groups)

        end_time = datetime.datetime.now()

        crime_provenance = \
            CrimeCentroidsProvenance(self.settings, database_helper=self.database_helper)
        crime_provenance.\
            update_provenance(full_provenance=full_provenance, start_time=start_time, end_time=end_time)

    def _load_prep_df(self):
        """
        Reads the pandas.DataFrame from the MongoDB collection, cleans it up, and returns it

        Returns
        -------
        pandas.DataFrame
        """
        df = self.database_helper.load_permanent_pandas('crime_incidents', cols=['location'])
        df = df.merge(df['location'].apply(lambda x: pandas.Series({'NEEDS_RECODE': x['needs_recoding'],
                                                                    'LONGITUDE': float(x['longitude']),
                                                                    'LATITUDE': float(x['latitude'])})),
                      left_index=True, right_index=True)
        df = df[(df['LONGITUDE'] != 0) & (df['LATITUDE'] != 0)].copy()

        return df

    @staticmethod
    def _kmeans_fit_predict(df, n_groups=15):
        """
        Uses a naive percentage change in Bayesian Information Criterion (BIC) to choose an optimal number of centers

        Parameters
        ----------
        df: pandas.DataFrame
        n_groups: int

        Returns
        -------
        pandas.DataFrame
        """
        k_range = range(2, n_groups)
        n = len(df)

        bic_last = -np.inf
        preds = None
        # centers = None

        for k in k_range:
            clust = cluster.KMeans(n_clusters=k, random_state=1)
            preds = clust.fit_predict(df[['LONGITUDE', 'LATITUDE']])
            # centers = clust.cluster_centers_
            sse = df[['LONGITUDE', 'LATITUDE']].groupby(preds).apply(sse_group, ['LONGITUDE', 'LATITUDE']).sum()
            bic = n * np.log(sse / n) + k * np.log(n)
            pct_improve = (bic - bic_last) / bic_last
            if pct_improve < .01:
                break
            bic_last = bic

        df['GROUP'] = preds
        df = df[['LONGITUDE', 'LATITUDE', 'GROUP']].copy()
        return df
