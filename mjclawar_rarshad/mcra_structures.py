from abc import abstractmethod


class APIQuery:
    @property
    @abstractmethod
    def data_namespace(self):
        pass

    @property
    @abstractmethod
    def data_entity(self):
        pass

    @abstractmethod
    def api_query(self):
        pass

    @abstractmethod
    def download_update_database(self):
        pass


class DataProvenance:
    @abstractmethod
    def document_flow(self):
        # TODO probably rename
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