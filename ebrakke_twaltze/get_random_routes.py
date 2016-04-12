import urllib.request
import prov.model
import datetime
import time
import uuid
import sys
import pymongo
import random
import json

exec(open('../pymongo_dm.py').read())
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('ebrakke_twaltze', 'ebrakke_twaltze')

def generate_routes(n):
    # Need to get a range to generate routes in
    dataset = repo['ebrakke_twaltze.dangerLevels']
    south_data_point = dataset.find().sort('lat', pymongo.ASCENDING).limit(1)
    north_data_point = dataset.find().sort('lat', pymongo.DESCENDING).limit(1)
    west_data_point = dataset.find().sort('lng', pymongo.ASCENDING).limit(1)
    east_data_point = dataset.find().sort('lng', pymongo.DESCENDING).limit(1)

    south = (south_data_point[0]['lat'], south_data_point[0]['lng'])
    north = (north_data_point[0]['lat'], north_data_point[0]['lng'])
    west = (west_data_point[0]['lat'], west_data_point[0]['lng'])
    east = (east_data_point[0]['lat'], east_data_point[0]['lng'])

    random_start_end_points = []
    for i in range(n):
        start = (random.uniform(south[0], north[0]), random.uniform(west[1], east[1]))
        end = (random.uniform(south[0], north[0]), random.uniform(west[1], east[1]))
        random_start_end_points.append([start, end])
    return random_start_end_points

def find_directions():
    repo.dropPermanent('randomDirections')
    repo.createPermanent('randomDirections')
    start_time = datetime.datetime.now()
    random_routes = generate_routes(100)

    url = 'https://maps.googleapis.com/maps/api/directions/json?origin={}&destination={}&mode=bicycling&alternatives=true'
    for (s,f) in random_routes:
        start = '{},{}'.format(s[0],s[1])
        finish = '{},{}'.format(f[0],f[1])
        this_url = url.format(start, finish)
        print('Requesting {}'.format(this_url))
        response = urllib.request.urlopen(this_url).read().decode('utf-8')
        r = json.loads(response)
        # Only insert if there is an actual path
        if r.get('routes'):
            repo['ebrakke_twaltze.randomDirections'].insert_many(r.get('routes'))
        time.sleep(2)
    end_time = datetime.datetime.now()
    do_prov(start_time, end_time)

def do_prov(start_time=None, end_time=None):
    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ebrakke_twaltze/')
    doc.add_namespace('dat', 'http://datamechanics.io/data/ebrakke_twaltze/')
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
    doc.add_namespace('log', 'http://datamechanics.io/log#')
    doc.add_namespace('gmap', 'https://maps.googleapis.com/maps/api/')

    script = doc.agent('alg: get_random_routes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
    directions = doc.entity('gmap:directions', {'prov:label': 'Directions Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension': 'json'})
    danger_levels = doc.entity('dat:dangerLevels', {'prov:label': 'Danger Levels', prov.model.PROV_TYPE:'ont:DataSet'})

    get_directions = doc.activity('log:a' + str(uuid.uuid4()), start_time, end_time)
    doc.wasAssociatedWith(get_directions, script)
    doc.usage(get_directions, directions, start_time, None, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?mode=bicycling&alternatives=true&origin=$start&destination=$finish'})

    random_directions = doc.entity('dat:randomDirections', {prov.model.PROV_LABEL:'Random Directions', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(random_directions, script)
    doc.wasGeneratedBy(random_directions, get_directions, end_time)
    doc.wasDerivedFrom(random_directions, directions, get_directions, get_directions, get_directions)
    doc.wasDerivedFrom(random_directions, danger_levels, get_directions, get_directions, get_directions)

    repo.record(doc.serialize())
    print(doc.get_provn())
    repo.logout()

if __name__ == '__main__':
    find_directions()
