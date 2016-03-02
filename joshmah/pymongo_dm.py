import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid
###############################################################################
## 
## pymongo_dm.py
##
## Script for extending the PyMongo database.Database class with customized
## methods for creating and dropping collections within repositories.
##
## Web:     datamechanics.org
## Version: 0.0.1.0
##
##

import json

pathToConfig = "../config.json"

def customElevatedCommand(db, f, arg, op = None):
    """
    Wrapper to create custom commands for managing the repository that
    require temporary elevation to an authenticated account with higher
    privileges.
    """
    config = json.loads(open(pathToConfig).read())
    user = db.command({"connectionStatus":1})['authInfo']['authenticatedUsers'][0]['user']
    if op != 'record' and arg.split(".")[0] != user:
        arg = user + '.' + arg
    db.logout()
    db.authenticate(config['admin']['name'], config['admin']['pwd'])
    result = f(arg, user, user)
    db.logout()
    db.authenticate(user, user)
    return result

def createTemporary(self, name):
    """
    Wrapper for creating a temporary repository collection
    that can be removed after a particular computation is complete.
    """
    return customElevatedCommand(self, self.system_js.createTemp, name)

def createPermanent(self, name):
    """
    Wrapper for creating a repository collection that should remain
    after it is derived.
    """
    return customElevatedCommand(self, self.system_js.createPerm, name)

def dropTemporary(self, name):
    """
    Wrapper for removing a temporary repository collection.
    """
    return customElevatedCommand(self, self.system_js.dropTemp, name)

def dropPermanent(self, name):
    """
    Wrapper for removing a permanent repository collection.
    """
    return customElevatedCommand(self, self.system_js.dropPerm, name)

def record(self, raw):
    """
    Wrapper for recording a provenance document. Since MongoDB
    does not support fields with the reserved "$" character, we
    replace this character with "@".
    """
    raw = raw.replace('"$"', '"@"')
    return customElevatedCommand(self, self.system_js.record, raw, 'record')

"""
We extend the pymongo Database class with the additional methods
defined above.
"""
pymongo.database.Database.createTemporary = createTemporary
pymongo.database.Database.createTemp = createTemporary
pymongo.database.Database.createPermanent = createPermanent
pymongo.database.Database.createPerm = createPermanent
pymongo.database.Database.dropTemporary = dropTemporary
pymongo.database.Database.dropTemp = dropTemporary
pymongo.database.Database.dropPermanent = dropPermanent
pymongo.database.Database.dropPerm = dropPermanent
pymongo.database.Database.record = record

## eof

