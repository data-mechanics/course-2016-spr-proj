# Make our db accessible over REST, with authentication.
from flask import Flask
from flask import request
from flask import send_from_directory
from bson.json_util import dumps
import pymongo
import os

app = Flask(__name__)

exec(open('../../pymongo_dm.py').read())
teamname = 'ciestu12_sajarvis'
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate(teamname, teamname)

@app.route('/stops')
def loadmap():
    '''Load the main file. To change the visualization change the
    js file.'''
    d = os.path.dirname(os.getcwd())
    return send_from_directory(os.path.join(d, 'vis'), 'optimal_stops.html')

@app.route('/get/<collection>')
def getthings(collection):
    '''Query the database from js in browser.'''
    fltr = {}
    for key in request.args.keys():
        fltr[key] = request.args[key]
    all_stops = repo['{}.{}'.format(teamname, collection)].find(fltr)
    return dumps(all_stops)

if __name__ == '__main__':
    app.run(debug=True)
