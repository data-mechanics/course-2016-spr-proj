import urllib.request
import datetime
import time
import uuid
import sys
import pymongo
import random
import json
import sys

import numpy as np

client = pymongo.MongoClient()
repo = client.repo

start = sys.argv[1].split(',')
end = sys.argv[2].split(',')
unique = sys.argv[3]

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

def find_routes(start, end):
	url = 'https://maps.googleapis.com/maps/api/directions/json?origin={}&destination={}&mode=bicycling&alternatives=true'
	start = '{},{}'.format(start[0],start[1])
	finish = '{},{}'.format(end[0],end[1])
	this_url = url.format(start, finish)

	response = urllib.request.urlopen(this_url).read().decode('utf-8')
	routes = json.loads(response).get('routes')

	# Remove the html_instructions and warnings keys from routes/steps
	for i, route in enumerate(routes):
		try:
			del routes[i]['warnings']
		except KeyError:
			pass

		for j, step in enumerate(route['legs'][0]['steps']):
			try:
				del routes[i]['legs'][0]['steps'][j]['html_instructions']
			except KeyError:
				pass

	return routes;

def incidents_on_path(path, incidentType):
	maxDistance = 5
	lats = [path[0][0], path[1][0]]
	lngs = [path[0][1], path[1][1]]
	north = max(lngs)
	south = min(lngs)
	east = min(lats)
	west = max(lats)

	dangerLevels = repo['ebrakke_twaltze.dangerLevels']
	incidents = dangerLevels.find({'type': incidentType, 'lat': {'$lt': west, '$gt': east}, 'lng': {'$lt': north, '$gt': south}}, {'_id': 0})

	i = []
	for incident in incidents:
		lat = incident['lat']
		lng = incident['lng']

		d = distance(path[0], path[1], np.array([lat, lng]))
		if d <= maxDistance:
			i.append(incident)

	return i

def run(start, end, incidentType):
	routes = find_routes(start, end)

	output = []
	for i, route in enumerate(routes):
		output.append({
			'route': route,
			'incidents': []
		})
		steps = route['legs'][0]['steps']

		for step in steps:
			s = [step['start_location']['lat'], step['start_location']['lng']]
			e = [step['end_location']['lat'], step['end_location']['lng']]

			output[i]['incidents'] += incidents_on_path([s, e], incidentType)

	with open('data/{}-data.json'.format(unique), 'w') as f:
		out = json.dumps(output)
		f.write(out)
		f.close()
	return json.dumps(output)

print(run(start, end, 'pothole'))
