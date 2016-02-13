"""
Setup mongo database.

Michael Clawar and Raaid Arshad.
"""
import pymongo

exec(open('../pymongo_dm.py').read())


def connect_repo():
    client = pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('mjclawar_rarshad', 'mjclawar_rarshad')
    return repo


def insert_db(dbname, thedata):
    assert isinstance(dbname, str)
    repo = connect_repo()
    repo.dropPermanent(dbname)
    repo.createPermanent(dbname)
    repo['mjclawar_rarshad.%s' % dbname].insert_many(thedata)
