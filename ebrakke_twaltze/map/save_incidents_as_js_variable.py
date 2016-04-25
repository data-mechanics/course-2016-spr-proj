import pymongo
import json

exec(open('../../pymongo_dm.py').read())
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('ebrakke_twaltze', 'ebrakke_twaltze')

dangerLevels = repo['ebrakke_twaltze.dangerLevels']
incidents = dangerLevels.find({}, {'_id':  0})

string = json.dumps(list(incidents));

with open('incidents.js', 'w+') as f:
	f.write("var incidentsJSON = '{}'".format(string))