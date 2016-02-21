"""
File: pandas.funcs.py

Description: Helper functions to make pandas play nice with MongoDB
Author(s): Raaid Arshad and Michael Clawar

Notes:
"""

import pandas
import pymongo

from mjclawar_rarshad.tools.database_helpers import DatabaseHelper


def read_mongo_collection(collection, database_helper, query=None):

    assert isinstance(database_helper, DatabaseHelper)
    repo = database_helper.connect_repo()

    if query is None:
        query = {}

    # Get query from MongoDB and construct pandas.DataFrame
    df = pandas.DataFrame(list(repo[collection].find(query)))

    return df
