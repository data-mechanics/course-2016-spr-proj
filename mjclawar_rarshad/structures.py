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
