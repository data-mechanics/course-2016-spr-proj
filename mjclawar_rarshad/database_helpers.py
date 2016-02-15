"""
Setup mongo database.

Michael Clawar and Raaid Arshad.
"""
import pymongo

exec(open('../pymongo_dm.py').read())


class DatabaseHelper:
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def connect_repo(self):
        client = pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(self.username, self.password)
        return repo

    def insert_permanent_db(self, database_name, json_data):
        assert isinstance(database_name, str)

        repo = self.connect_repo()
        repo.dropPermanent(database_name)
        repo.createPermanent(database_name)
        repo['%s.%s' % (self.username, database_name)].insert_many(json_data)

        repo.logout()

    def record(self, json_data):
        repo = self.connect_repo()
        repo.record(json_data)

        repo.logout()
