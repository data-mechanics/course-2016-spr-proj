/* ****************************************************************************
** 
** setup.js
**
** Script for setting up and initializing a Data Mechanics Repository instance
** within MongoDB.
**
** This script will not overwrite any existing user data if setup has already
** been run on a MongoDB instance, but it will add new users if they appear.
** To clean an existing instance, use the 'reset.js' script.
**
**   Web:     datamechanics.org
**   Version: 0.0.1
**
*/

// Configuration.
var pwd = "example";

// Create an administrator with full privileges.
db = new Mongo().getDB("admin");
db.dropUser("admin");
if (db.system.users.find({user:'admin'}).count() > 0) {
  print("Found 'admin' user in admin database; not creating a new user.");
} else {
  db.createUser({
    user: "admin",
    pwd: pwd,
    roles: [
        "userAdminAnyDatabase",
        "dbAdminAnyDatabase",
        "readWriteAnyDatabase",
        {role: "readWrite", db:"dmr"}
      ]
  });
}

// Create repository users if they are not already present.
db = new Mongo().getDB("dmr");
db.createUser({
  user: "admin",
  pwd: pwd,
  roles: [
      {role: "readWrite", db:"dmr"}
    ]
});
listFiles().forEach(function(f) {
  if (f.isDirectory) {
    var userName = f.baseName;
    db.dropUser(userName);
    if (db.system.users.find({user:userName}).count() > 0) {
      print("Found '" + userName + "' user in admin database; not creating a new user.");
    } else {
      var user = {
          user: userName,
          pwd: pwd,
          roles: [
              {role: "read", db: "admin"},
              {role: "read", db: "dmr", collection: "system.js"}
            ]
        };
      db.createUser(user);
    }
  }
});

// Save the custom server-side functions.
db.system.js.save({_id: "createTemporary", value:
  (function(user, pwd, collName) {
    var db = new Mongo().getDB("admin");
    db.auth("admin", "example");
    var dmr = new Mongo().getDB("dmr");
    dmr.createCollection("registry");
    dmr.registry.insert({name:collName, lifespan:'temporary'});
    dmr.createCollection(collName);
    dmr.auth(user, pwd);
  })
});
db.system.js.save({_id: "createPermanent", value:
  (function(user, pwd, collName) {
    var dmr = new Mongo().getDB("dmr");
    dmr.auth("admin", "example");
    dmr.createCollection("registry");
    dmr.registry.insert({name:collName, lifespan:'permanent'});
    dmr.createCollection(collName);
    dmr.auth(user, pwd);
  })
});
print("Saved custom functions.");

/* eof */