import prov.model
import datetime
import time
import uuid
import sys
import pymongo
import random
import json

from numpy import arccos, array, dot, pi
from numpy.linalg import det, norm

exec(open('../pymongo_dm.py').read())
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('ebrakke_twaltze', 'ebrakke_twaltze')

def distance(A, B, P):
	""" segment line AB, point P, where each one is an array([x, y]) """
	if all(A == P) or all(B == P):
		return 0
	if arccos(dot((P - A) / norm(P - A), (B - A) / norm(B - A))) > pi / 2:
		return norm(P - A)
	if arccos(dot((P - B) / norm(P - B), (A - B) / norm(A - B))) > pi / 2:
		return norm(P - B)
	return abs(dot(A - B, P[::-1]) + det([A, B])) / norm(A - B)

def incidents_on_path(path, incidentType):
	maxDistance = 100
	lats = [path[0][0], path[1][0]]
	lngs = [path[0][1], path[1][1]]
	north = max(lngs)
	south = min(lngs)
	east = min(lats)
	west = max(lats)

	dangerLevels = repo['ebrakke_twaltze.dangerLevels']
	incidents = dangerLevels.find({'type': incidentType, 'lat': {'$lt': west, '$gt': east}, 'lng': {'$lt': north, '$gt': south}})

	i = []
	for incident in incidents:
		lat = incident['lat']
		lng = incident['lng']

		d = distance(path[0], path[1], array([lat, lng]))
		if d <= maxDistance:
			i.append(incident)

	return i

def calculate_average_incident(incidentType):
	directions = repo['ebrakke_twaltze.randomDirections']
	routes = directions.find()

	count = 0;
	for route in routes:
		steps = route['legs'][0]['steps']

		for step in steps:
			start = array([step['start_location']['lat'], step['start_location']['lng']])
			end = array([step['end_location']['lat'], step['end_location']['lng']])

			count += len(incidents_on_path([start, end], incidentType))
			print(count)

	return count / incidents.count()

print(calculate_average_incident('pothole'))
