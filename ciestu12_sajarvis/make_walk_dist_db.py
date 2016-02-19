"""
Query Google maps for walking distances between T stops.

Get walking distance and time from every Green Line stop to every other.

It's a combination of GPS coordinates and line information from T data and
distances from Google Maps API.

Relies on these collections already existing in the database:
    t_branch_info
    t_stop_locations

It also takes quite a while, due to rate limiting at the free tier of Google's
API.
"""
import urllib.request
import json
import pymongo
import prov.model
import time
import datetime
import uuid
import sys

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

teamname = 'ciestu12_sajarvis'
# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate(teamname, teamname)

startTime = datetime.datetime.now()

out_coll = 'green_line_walking_distances'
repo.dropPermanent(out_coll)
repo.createPermanent(out_coll)

def product(X, Y):
    '''Cartesian product of sets X and Y'''
    return [(a, b) for a in X for b in Y]

def request_get_json(source_lat, source_lon, dest_lat, dest_lon):
    '''Make the request to Google Maps for the walking distance from
    source to dest.'''
    url = 'https://maps.googleapis.com/maps/api/distancematrix/json?origins={},{}&destinations={},{}&mode=walking&units=imperial'.format(
        source_lat, source_lon, dest_lat, dest_lon)
    response = urllib.request.urlopen(url).read().decode("utf-8")
    return json.loads(response)

def get_unique_lines():
    '''From the existing t_branch_info collection, get list of unique lines.'''
    # This collection must already exist.
    all_lines = repo['{}.{}'.format(teamname, 't_branch_info')].find({})
    return set([x['line'] for x in all_lines])

def get_stops_for_line(line):
    '''From the existing t_branch_info collection, all stops on given line.'''
    # This collection must already exist.
    return list(repo['{}.{}'.format(teamname, 't_branch_info')].find({'line':l}))

for l in get_unique_lines():
    line_stops = []
    for s in [s for s in get_stops_for_line(l)]:
        stop_id = s['stop_id']
        # There should be exactly 1 associated document in the GPS db.
        coords_doc = repo['{}.{}'.format(teamname,  't_stop_locations')].find({'stop_id':stop_id})[0]
        # Append a tuple of the lat,lon, and this stop ID.
        line_stops.append((coords_doc['stop_lat'], coords_doc['stop_lon'], stop_id))

    for r in product(line_stops, line_stops):
        data = request_get_json(r[0][0], r[0][1], r[1][0], r[1][1])
        print(data)
        repo['{}.{}'.format(teamname, out_coll)].insert_one(
            { 'source_id' : r[0][2], \
              'dest_id' : r[1][2], \
              'line' : l, \
              'dist_val_ft' : data['rows'][0]['elements'][0]['distance']['value'], \
              'dist_text' : data['rows'][0]['elements'][0]['distance']['text'], \
              'duration_val_sec' : data['rows'][0]['elements'][0]['duration']['value'], \
              'duration_text' : data['rows'][0]['elements'][0]['duration']['text'] } )

        # We can only have 100 elements per request, and 100 per 10 seconds, so
        # need to rate limit this a bit.
        time.sleep(1.5)

endTime = datetime.datetime.now()

# TODO provenance data
