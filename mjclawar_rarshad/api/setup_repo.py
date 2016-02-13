"""
Setup mongo database.

Michael Clawar and Raaid Arshad.
"""
import pymongo


client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('mjclawar_rarshad', 'mjclawar_rarshad')


def connect_to_db():
    pass


def insert_permanent_db():
    pass


