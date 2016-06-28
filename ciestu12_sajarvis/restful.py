# Make our db accessible over REST, with authentication to the db.
#
# Inline notes provide some commentary on what it's doing, as well as thoughts
# on how to potentially expand this concept to use for the entire platform.

from flask import Flask
from flask import request
from flask import send_from_directory
from bson.json_util import dumps
import dml
import os

app = Flask(__name__)

# To use as a framework for the entire system, there would be a universally
# usable read-only account to statically configure and use here. Or, else,
# requests could be made in sessions based on the requesting and authenticated
# team.
teamname = 'ciestu12_sajarvis'
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate(teamname, teamname)

# If there's a defined structure to each team's foler (i.e. html and resources
# in alice_bob/html) all paths could be enumerated and build a directory listing
# for the root URL.
@app.route('/')
def index():
    return """Nothing here, try
        <a href=\"./stops\">/stops</a> or
        <a href=\"./utility\">/utility</a>.
        <br>
        For some database meta statistics, see
        <a href=\"./meta\">/meta</a>.
        """

# These specific routes load static html/js files. If used for the entire
# platform, would need to manipulate paths based on particular teamname.
@app.route('/stops')
def loadmap():
    '''Load the static file. To change the visualization change the js file.'''
    d = os.path.dirname(os.getcwd())
    return send_from_directory(os.path.join(d, 'vis'), 'optimal_stops.html')

@app.route('/utility')
def loadutil():
    '''Load the static file. To change the visualization change the js file.'''
    d = os.path.dirname(os.getcwd())
    return send_from_directory(os.path.join(d, 'vis'), 'utility.html')

@app.route('/meta')
def loadstats():
    '''Load the static file. To change the visualization change the js file.'''
    d = os.path.dirname(os.getcwd())
    return send_from_directory(os.path.join(d, 'vis'), 'collection_stats.html')

# This route serves as the general "get an arbitrary collection" interface.
# Can provide filters to find() by adding a query to the URL.
@app.route('/get/<collection>')
def getthings(collection):
    '''Query the database from js in browser.'''
    # If you have a collection with 'name':<name> in the documents, filter
    # on a name with query args like /collection?name=<name>
    args = {}
    for k in request.args.keys():
        try:
            # Mongo will not be flexible with types, will need to make this a
            # little smarter if supporting more varying types.
            args[k] = int(request.args[k])
        except ValueError:
            args[k] = request.args[k]
    all_stops = repo['{}.{}'.format(teamname, collection)].find(args)
    return dumps(all_stops)

@app.route('/getstats')
def getstats():
    '''Dumps statistics on all collections in the database, all authors.'''
    colls = []
    for coll in repo.collection_names():
        colls.append(repo.command('collstats', '{}'.format(coll)))
    return dumps(colls)

@app.route('/getprov')
def getprov():
    '''Dumps provenance for all collections, all authors.'''
    provs = []
    for prov in repo['_provenance'].find():
        provs.append(prov)
    return dumps(provs)


if __name__ == '__main__':
    # NOTE don't run debug on final server, allows Python execution in browser.
    app.run(debug=True)
