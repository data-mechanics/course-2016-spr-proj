from abc import abstractmethod
import prov.model


class APIQuery:
    @abstractmethod
    def download_update_database(self):
        pass


class MCRASSettings:
    @property
    @abstractmethod
    def data_namespace(self):
        pass

    @property
    @abstractmethod
    def data_entity(self):
        pass

    @property
    @abstractmethod
    def base_url(self):
        pass


class MCRASProvenance:
    pass


class Namespace:
    def __init__(self, name, link):
        self.name = name
        self.link = link

BDP_NAMESPACE = Namespace(name='bdp', link='https://data.cityofboston.gov/resource')
ALG_NAMESPACE = Namespace(name='alg', link='http://datamechanics.io/algorithm/mjclawar_rarshad/')
DAT_NAMESPACE = Namespace(name='dat', link='http://datamechanics.io/data/mjclawar_rarshad/')
ONT_NAMESPACE = Namespace(name='ont', link='http://datamechanics.io/ontology#')
LOG_NAMESPACE = Namespace(name='log', link='http://datamechanics.io/log#')


PROVENANCE_ONT_RETRIEVAL = '%s:Retrieval' % ONT_NAMESPACE.name
PROV_ONT_QUERY = '%s:Query' % ONT_NAMESPACE.name
PROV_ONT_EXTENSION = '%s:Extension' % ONT_NAMESPACE.name
PROV_ONT_DATASET = '%s:DataSet' % ONT_NAMESPACE.name

PROVENANCE_PYTHON_SCRIPT = {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], PROV_ONT_EXTENSION: 'py'}
