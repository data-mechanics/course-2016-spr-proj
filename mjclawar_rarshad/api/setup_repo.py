"""
Setup mongo database.

Michael Clawar and Raaid Arshad.
"""
import pymongo


client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('mjclawar_rarshad','mjclawar_rarshad')