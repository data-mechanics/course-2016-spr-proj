import prov.model
import datetime
import time
import uuid
import sys
import dml
import random
import json

import numpy as np
from prov.serializers import provjson

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('ebrakke_twaltze', 'ebrakke_twaltze')

def distance(start, end, point):
    """ Return the distance from a point to a line in meters """
    lat1, lng1 = start
    lat2, lng2 = end
    lat3, lng3 = point
    R = 6371000.0

    y = np.sin(lng3 - lng1) * np.cos(lat3)
    x = np.cos(lat1) * np.sin(lat3) - np.sin(lat1) * np.cos(lat3) * np.cos(lat3 - lat1)
    bearing1 = np.rad2deg(np.arctan2(y,x))
    bearing1 = 360 - (bearing1 - 360 % 360)

    y2 = np.sin(lng2 - lng1) * np.cos(lat2)
    x2 = np.cos(lat1) * np.sin(lat2) - np.sin(lat1) * np.cos(lat2) * np.cos(lat2 - lat1)
    bearing2 = np.rad2deg(np.arctan2(y2,x2))
    bearing2 = 360 - (bearing2 - 360 % 360)

    lat1Rad = np.deg2rad(lat1)
    lat3Rad = np.deg2rad(lat3)
    dLong = np.deg2rad(lng3 - lng1)

    distance13 = np.arccos(np.sin(lat1Rad) * np.sin(lat3Rad) + np.cos(lat1Rad)*np.cos(lat3Rad)*np.cos(dLong)) * R
    min_distance = np.fabs(np.arcsin(np.sin(distance13/R) * np.sin(np.deg2rad(bearing1) - np.deg2rad(bearing2))) * R)

    return min_distance

def incidents_on_path(path, incidentType):
	maxDistance = .33
	lats = [path[0][0], path[1][0]]
	lngs = [path[0][1], path[1][1]]
	north = max(lngs)
	south = min(lngs)
	east = min(lats)
	west = max(lats)

	dangerLevels = repo['ebrakke_twaltze.dangerLevels']
	incidents = dangerLevels.find({'type': incidentType, 'lat': {'$lt': west, '$gt': east}, 'lng': {'$lt': north, '$gt': south}})

	i = []
	count = 0
	for incident in incidents:
		lat = incident['lat']
		lng = incident['lng']

		d = distance(path[0], path[1], np.array([lat, lng]))
		if d <= maxDistance:
			i.append(incident)
	return i

def calculate_average_incident(incidentType):
    repo.dropPermanent('randomDirectionIncidentCount')
    repo.createPermanent('randomDirectionIncidentCount')

    incidentCount = repo['ebrakke_twaltze.randomDirectionIncidentCount']
    directions = repo['ebrakke_twaltze.randomDirections']
    routes = directions.find()
    start_time = datetime.datetime.now()

    for i,route in enumerate(routes):
        if list(incidentCount.find({'_id': route['_id']})):
        	continue
        print("Starting route " + str(i))
        steps = route['legs'][0]['steps']
        distance = 0
        potholes = []

        for step in steps:
            start = np.array([step['start_location']['lat'], step['start_location']['lng']])
            end = np.array([step['end_location']['lat'], step['end_location']['lng']])
            distance += step['distance']['value']
            potholes += incidents_on_path([start, end], incidentType)
        incidentCount.insert({'distance': distance, 'count': len(potholes), 'incidents': potholes})
    end_time = datetime.datetime.now()
    do_prov(start_time, end_time)

def do_prov(start_time=None, end_time=None):
    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ebrakke_twaltze/')
    doc.add_namespace('dat', 'http://datamechanics.io/data/ebrakke_twaltze/')
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
    doc.add_namespace('log', 'http://datamechanics.io/log#')

    script = doc.agent('alg:average_incidents_on_path', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
    random_routes = doc.entity('dat:randomDirections', {prov.model.PROV_LABEL:'Random Directions', prov.model.PROV_TYPE:'ont:DataResource'})
    danger_levels = doc.entity('dat:dangerLevels', {prov.model.PROV_LABEL:'Danger Levels', prov.model.PROV_TYPE:'ont:DataResource'})

    get_potholes_along_route = doc.activity('log:a' + str(uuid.uuid4()), start_time, end_time, {prov.model.PROV_LABEL: 'Find potholes along a route'})
    doc.wasAssociatedWith(get_potholes_along_route, script)
    doc.usage(get_potholes_along_route, random_routes, start_time, None, {prov.model.PROV_TYPE:'ont:Computation'})
    doc.usage(get_potholes_along_route, danger_levels, start_time, None, {prov.model.PROV_TYPE:'ont:Computation'})

    incidents = doc.entity('dat:randomDirectionIncidentCount', {prov.model.PROV_LABEL:'Random Direction incident Count', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(incidents, script)
    doc.wasGeneratedBy(incidents, get_potholes_along_route, end_time)
    doc.wasDerivedFrom(incidents, random_routes, get_potholes_along_route, get_potholes_along_route, get_potholes_along_route)
    doc.wasDerivedFrom(incidents, danger_levels, get_potholes_along_route, get_potholes_along_route, get_potholes_along_route)

    repo.record(doc.serialize())
    #plan = json.loads(open('plan.json', 'r').read())
    #provjson.decode_json_document(plan, doc)
    #open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
    print(doc.get_provn())

calculate_average_incident('pothole')
#do_prov()
