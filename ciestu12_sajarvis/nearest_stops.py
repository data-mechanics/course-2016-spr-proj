"""
Use the 'green_line_walking_distances' to figure out the time and distance
to the nearest neighbor stop on the line, for each stop on the line.
"""
import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

teamname = 'ciestu12_sajarvis'
# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate(teamname, teamname)

startTime = datetime.datetime.now()

def map(R, f):
    '''Map set R according to function f. R is a set of tuples.'''
    return [t for (k, v) in R for t in f(k, v)]

def reduce(R, f):
    '''Reduce the set R with function f.'''
    keys = {k for (k, v) in R}
    return [f(k, [v for (k1, v) in R if k == k1]) for k in keys]

# Transformation on the 'green_line_walking_distances' collection
all_stops = repo['{}.{}'.format(teamname, 'green_line_walking_distances')].find({})
# Make a list of tuples to start, with all the information we'd
# like to consider during operation and for the resulting data set.
stops = [(s['source_id'], (s['line'], s['dest_id'], s['dist_val_ft'], s['duration_val_sec'])) for s in all_stops]

# Map together all results for each stop on each particular line.
distances = map(stops, lambda k,v: [((k,v[0]), v)])

# Reduce on the minimum *time* distance walking to the other stops on
# the line. Line is not as complex as it looks, more just wordy.
# We could refactor to not use anonymous lambdas..
shortests = reduce(distances, lambda k,vs: (k[0], min([v for v in vs if v[1] != k[0]], key = lambda t : t[3])))

# Put the resulting list of tuples into a JSON-friendly dictionary.
elements = [{'stop':k, 'line':v[0], 'nearest':v[1], 'distance_ft':v[2], 'time_sec':v[3]} for k,v in shortests]

nearest_coll = 'nearest_stops'
repo.dropPermanent(nearest_coll)
repo.createPermanent(nearest_coll)
repo['{}.{}'.format(teamname, nearest_coll)].insert_many(elements)

endTime = datetime.datetime.now()

# TODO provenance data and recording

repo.logout()
