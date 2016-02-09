from abc import ABCMeta, abstractmethod

class APIQuery:
    @abstractmethod
    def api_query(self):
        pass

    @abstractmethod
    def download_update_database(self):
        pass