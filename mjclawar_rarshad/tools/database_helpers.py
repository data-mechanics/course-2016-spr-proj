"""
Helpers for the project's MongoDB collections

Michael Clawar and Raaid Arshad.
"""

import pymongo


class DatabaseHelper:
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def connect_repo(self):
        """
        Connects to the MongoDB repo

        Returns
        -------
        """
        client = pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(self.username, self.password)
        return repo

    def insert_permanent_collection(self, collection_name, json_data):
        """
        Logs in, inserts a permanent collection to the MongoDB, and logs out

        Parameters
        ----------
        collection_name: str
            Name of the permanent collection to insert
        json_data: json
            The json data to insert into the permanent collection

        Returns
        -------
        """
        assert isinstance(collection_name, str)

        repo = self.connect_repo()
        repo.dropPermanent(collection_name)
        repo.createPermanent(collection_name)
        repo['%s.%s' % (self.username, collection_name)].insert_many(json_data)

        repo.logout()

    def insert_temporary_collection(self, collection_name, json_data):
        """
        Logs in, inserts a temporary collection to the MongoDB, and logs out

        Parameters
        ----------
        collection_name: str
            Name of the temporary collection to insert
        json_data: json
            The json data to insert into the permanent collection

        Returns
        -------
        """
        assert isinstance(collection_name, str)

        repo = self.connect_repo()
        repo.dropTemporary(collection_name)
        repo.createTemporary(collection_name)
        repo['%s.%s' % (self.username, collection_name)].insert_many(json_data)

        repo.logout()

    def record(self, json_data):
        # TODO This doesn't work, ask Prof. Lapets
        repo = self.connect_repo()
        repo.record(json_data)

        repo.logout()
