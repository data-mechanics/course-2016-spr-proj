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
** Since the distinct user accounts are only for sanity checking and not
** security, their passwords are not material.
**
**   Web:     datamechanics.org
**   Version: 0.0.1
**
*/

// Load the configuration file.
var config = JSON.parse(cat("config.json"));

// Create role capable of evaluating stored functions.
db = new Mongo().getDB('admin');
db.dropRole("evaluator");
db.createRole({
  role: 'evaluator',
  privileges: [{resource:{anyResource:true}, actions:['anyAction']}],
  roles: []
});

// Create administration account for repository.
db = new Mongo().getDB(config.repo.name);
db.dropUser(config.admin.name);
db.createUser({
  user: config.admin.name, 
  pwd: config.admin.pwd, 
  roles: [
      {role: "evaluator", db:'admin'},
      {role: "userAdmin", db: config.repo.name},
      {role: "readWrite", db: config.repo.name}
    ]
});

// Create repository users if they are not already present.
listFiles().forEach(function(f) {
  if (f.isDirectory) {
    var userName = f.baseName;
    if (db.system.users.find({user:userName}).count() > 0) {
      print("Found '" + userName + "' user in admin database; not creating a new user.");
    } else {
      db.dropRole(userName);
      db.createRole({
          role: userName,
          privileges: [],
          roles: [{role: "read", db: config.repo.name}]
        });
      db.dropUser(userName);
      db.createUser({
          user: userName,
          pwd: userName, // Accounts/passwords are for convenience/sanity checks and not security.
          roles: [{role: userName, db: config.repo.name}]
        });
    }
  }
});

// Save the custom server-side functions.
var currentUser =
    // Return the current user as a string.
  (function() {
    return db.runCommand({connectionStatus:1}).authInfo.authenticatedUsers[0].user;
  });
db.system.js.save({_id:"currentUser", value:currentUser});

var createCreate =
  (function(lifespan) {
    // Build the function that creates a new collection and
    // grants the user that created it write permissions.
    return eval(
          "(function(collName, user, pwd) {"
        + "  /* By default, use current user. */"
        + "  if (user == null)"
        + "    user = currentUser();"
        + "  if (pwd == null)"
        + "    pwd = user;"
        + "  /* Validate collection name as <user>.<collection>. */"
        + "  if (collName.split('.')[0] != user)"
        + "    collName = user + '.' + collName;"
        + "  var repo = new Mongo().getDB('" + config.repo.name + "');"
        + "  repo.auth('" + config.admin.name + "', '" + config.admin.pwd + "');"
        + "  repo.createCollection('_registry');"
        + "  repo.getCollection('_registry').insert({name:collName, lifespan:'" + lifespan + "', creator:user});"
        + "  repo.createCollection(collName);"
        + "  repo.runCommand({grantPrivilegesToRole:user,"
        + "    privileges: ["
        + "        { resource:{db:'" + config.repo.name + "', collection:collName },"
        + "          actions:['find','insert','remove','update','createIndex'] }"
        + "      ]"
        + "  });"
        + "  repo.auth(user, pwd);"
        + "  return collName;"
        + "})"
      ); // eval()
  });
db.system.js.save({_id:"createTemporary", value:createCreate("temporary")});
db.system.js.save({_id:"createTemp", value:createCreate("temporary")});
db.system.js.save({_id:"createPermanent", value:createCreate("permanent")});
db.system.js.save({_id:"createPerm", value:createCreate("permanent")});

var createDrop =
  (function(lifespan) {
    // Build the function that drops a collection.
    return eval(
          "(function(collName, user, pwd) {"
        + "  /* By default, use current user. */"
        + "  if (user == null)"
        + "    user = currentUser();"
        + "  if (pwd == null)"
        + "    pwd = user;"
        + "  /* Validate collection name as <user>.<collection>. */"
        + "  if (collName.split('.')[0] != user)"
        + "    collName = user + '.' + collName;"
        + "  var repo = new Mongo().getDB('" + config.repo.name + "');"
        + "  repo.auth('" + config.admin.name + "', '" + config.admin.pwd + "');"
        + "  repo.getCollection('_registry').remove({name:collName});"
        + "  repo[collName].drop();"
        + "  repo.auth(user, pwd);"
        + "  return collName;"
        + "})"
      ); // eval()
  });
db.system.js.save({_id:"dropTemporary", value:createDrop()});
db.system.js.save({_id:"dropTemp", value:createDrop()});
db.system.js.save({_id:"dropPermanent", value:createDrop()});
db.system.js.save({_id:"dropPerm", value:createDrop()});

var createRecord =
  (function() {
    // Build the function that stores a provenance record.
    return eval(
          "(function(doc, user, pwd) {"
        + "  /* By default, use current user. */"
        + "  if (user == null)"
        + "    user = currentUser();"
        + "  if (pwd == null)"
        + "    pwd = user;"
        + "  var repo = new Mongo().getDB('" + config.repo.name + "');"
        + "  repo.auth('" + config.admin.name + "', '" + config.admin.pwd + "');"
        + "  repo.createCollection('_provenance');"
        + "  var result = repo.getCollection('_provenance').insert(JSON.parse(doc));"
        + "  repo.auth(user, pwd);"
        + "  return result;"
        + "})"
      ); // eval()
  });
db.system.js.save({_id:"record", value:createRecord()});

var createCleanCollections =
  (function() {
    // Build the function that cleans out all the temporary collections.
    // TODO: A separate function to clean out out-of-date role privileges.
    return eval(
          "(function() {"
        + "  var repo = new Mongo().getDB('" + config.repo.name + "');"
        + "  repo.getCollection('_registry').find().toArray().forEach(function(c) {"
        + "    if (  c.lifespan == 'temporary'"
        + "       && (currentUser() == c.creator || currentUser() == 'admin')"
        + "       ) {"
        + "      repo.getCollection('_registry').remove({name:c.name});"
        + "      repo[c.name].drop();"
        + "    }"
        + "  });"
        + "  return 1;"
        + "})"
      ); // eval()
  });
db.system.js.save({_id:"cleanCollections", value:createCleanCollections()});

print('Saved custom functions and scripts to "' + config.repo.name + '".');

/* eof */