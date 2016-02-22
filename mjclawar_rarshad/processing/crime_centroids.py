"""
File: crime_centroids.py

Description: Calculates centroids for crime committed in Boston using k-means
Author(s): Raaid Arshad and Michael Clawar

Notes:
"""


import numpy as np
import pandas
from sklearn import cluster

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
    distances = spherical_dist(df[columns].values, np.array([df[columns].mean().values]))
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
        return mcras.DAT_NAMESPACE.link + 'crime_incidents'


class CrimeCentroidsProvenance(MCRASProvenance):
    def __init__(self, settings, database_helper):
        assert isinstance(settings, CrimeCentroidsSettings)
        assert isinstance(database_helper, DatabaseHelper)
        self.settings = settings
        self.database_helper = database_helper

    def update_provenance(self, start_time, end_time):
        """
        Writes a ProvDoc for the crime_centroids.py script and saves to the collection

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
                                   {prov.model.PROV_LABEL: 'Crime Centroids',
                                    prov.model.PROV_TYPE: mcras.PROV_ONT_DATASET})

        prov_doc.wasAttributedTo(data_doc, this_script)
        prov_doc.wasGeneratedBy(data_doc, this_run, end_time)
        prov_doc.wasDerivedFrom(data_doc, resource, this_run, this_run, this_run)

        # TODO figure out with record
        prov_obj.write_provenance_json()
        # self.database_helper.record(prov_doc.serialize())


class CrimeCentroidsProcessor(MCRASProcessor):
    def __init__(self, settings, database_helper):
        assert isinstance(settings, CrimeCentroidsSettings)
        assert isinstance(database_helper, DatabaseHelper)
        self.settings = settings
        self.database_helper = database_helper

    def run_processor(self):
        start_time = datetime.datetime.now()
        df = self._load_prep_df()
        df_groups = self._kmeans_fit_predict(df, n_groups=15)

        self.database_helper.insert_permanent_pandas(self.settings.data_entity, df_groups)

        end_time = datetime.datetime.now()

        crime_provenance = CrimeCentroidsProvenance(self.settings, database_helper=self.database_helper)
        crime_provenance.update_provenance(start_time=start_time, end_time=end_time)

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
