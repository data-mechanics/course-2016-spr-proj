"""
File: school_scatter.py

Description: Returns a json object with the data to plot the distance to the nearest school against property value.
Author(s): Raaid Arshad and Michael Clawar

Notes:
"""

import datetime
import uuid
import prov
import json

from mjclawar_rarshad.reference import mcra_structures as mcras
from mjclawar_rarshad.reference.mcra_structures import MCRASProcessor, MCRASSettings, MCRASProvenance
from mjclawar_rarshad.tools.database_helpers import DatabaseHelper
from mjclawar_rarshad.tools.provenance import ProjectProvenance
import plotly.graph_objs as gobjs


class SchoolScatterSettings(MCRASSettings):
    @property
    def data_namespace(self):
        return mcras.DAT_NAMESPACE

    @property
    def data_entity(self):
        return 'school_scatter'

    @property
    def agent(self):
        return '%s:%s' % (mcras.ALG_NAMESPACE.name, self.data_entity)

    @property
    def base_url(self):
        return 'school_distances'


class SchoolScatterProvenance(MCRASProvenance):
    def __init__(self, settings, database_helper):
        assert isinstance(settings, SchoolScatterSettings)
        assert isinstance(database_helper, DatabaseHelper)
        self.settings = settings
        self.database_helper = database_helper

    def update_provenance(self, full_provenance=False, start_time=None, end_time=None):
        """
        Writes a ProvDoc for the school_scatter.py script and saves to the collection

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

        resource_schools = \
            prov_doc.entity('%s:%s' % (self.settings.data_namespace.name, self.settings.base_url))

        if full_provenance:
            this_run = prov_doc.activity('%s:a%s' % (mcras.LOG_NAMESPACE.name, str(uuid.uuid4())))
            prov_doc.used(this_run, resource_schools)
        else:
            this_run = prov_doc.activity('%s:a%s' % (mcras.LOG_NAMESPACE.name, str(uuid.uuid4())), start_time, end_time,
                                         {prov.model.PROV_TYPE: mcras.PROV_ONT_EXTENSION})

        prov_doc.wasAssociatedWith(this_run, this_script)

        data_doc = prov_doc.entity('%s:%s' % (mcras.DAT_NAMESPACE.name, self.settings.data_entity),
                                   {prov.model.PROV_LABEL: 'School Scatter',
                                    prov.model.PROV_TYPE: mcras.PROV_ONT_DATASET})

        prov_doc.wasAttributedTo(data_doc, this_script)
        if full_provenance:
            prov_doc.wasGeneratedBy(data_doc, this_run)
        else:
            prov_doc.wasGeneratedBy(data_doc, this_run, end_time)

        prov_doc.wasDerivedFrom(data_doc, resource_schools, this_run)

        if full_provenance:
            prov_obj.write_provenance_json()
        else:
            self.database_helper.record(prov_doc.serialize())


class SchoolScatterProcessor(MCRASProcessor):
    def __init__(self, settings, database_helper):
        assert isinstance(settings, SchoolScatterSettings)
        assert isinstance(database_helper, DatabaseHelper)
        self.settings = settings
        self.database_helper = database_helper

    def run_processor(self, full_provenance=False):
        start_time = datetime.datetime.now()
        trace = self._generate_trace()

        with open('school_scatter.json', 'w') as f:
            json.dump(trace, f)

        end_time = datetime.datetime.now()

        school_scatter_provenance = SchoolScatterProvenance(self.settings, database_helper=self.database_helper)
        school_scatter_provenance.update_provenance(full_provenance=full_provenance, start_time=start_time,
                                                    end_time=end_time)

    def _generate_trace(self):

        df = self.database_helper.load_permanent_pandas('school_distances', cols=['AV_TOTAL', 'LIVING_AREA',
                                                                                  'MIN_DISTANCE'])

        df['AV_TOTAL'] = df['AV_TOTAL'].astype(float)
        df['LIVING_AREA'] = df['LIVING_AREA'].astype(float)
        df = df[(df['AV_TOTAL'] != 0) & (df['LIVING_AREA'] != 0)]

        df['PER_SQFT'] = df['AV_TOTAL'] / df['LIVING_AREA']
        df = df[df['PER_SQFT'] < 500]
        df = df[df['PER_SQFT'] >= 50]

        trace = gobjs.Scatter(x=list(df['MIN_DISTANCE'].values), y=list(df['PER_SQFT'].values), mode='markers')
        return trace
