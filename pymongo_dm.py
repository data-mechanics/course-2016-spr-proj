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

def customElevatedCommand(db, f, name):
    user = db.command({"connectionStatus":1})['authInfo']['authenticatedUsers'][0]['user']
    if name.split(".")[0] != user:
        name = user + '.' + name
    db.logout()
    db.authenticate('admin', 'example')
    result = f(name,user)
    db.logout()
    db.authenticate(user, user)
    return result

def createTemporary(self, name):
    return customElevatedCommand(self, self.system_js.createTemp, name)

def createPermanent(self, name):
    return customElevatedCommand(self, self.system_js.createPerm, name)

def dropTemporary(self, name):
    return customElevatedCommand(self, self.system_js.dropPerm, name)

def dropPermanent(self, name):
    return customElevatedCommand(self, self.system_js.dropPerm, name)

pymongo.database.Database.createTemporary = createTemporary
pymongo.database.Database.createTemp = createTemporary
pymongo.database.Database.createPermanent = createPermanent
pymongo.database.Database.createPerm = createPermanent
pymongo.database.Database.dropTemporary = dropTemporary
pymongo.database.Database.dropTemp = dropTemporary
pymongo.database.Database.dropPermanent = dropPermanent
pymongo.database.Database.dropPerm = dropPermanent

## eof