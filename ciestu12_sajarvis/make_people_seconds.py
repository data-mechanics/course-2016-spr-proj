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

STOP_TIME = 60

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

teamname = 'ciestu12_sajarvis'
# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate(teamname, teamname)

startTime = datetime.datetime.now()

def product(R, S):
    return [(t,u) for t in R for u in S]

# get DB for population and nearest stop
stop_pop = repo['{}.{}'.format(teamname, 'boarding_counts')].find({})
nearest_stops = repo['{}.{}'.format(teamname, 'nearest_stops')].find({})

nearest = [ (s['stop'], s['line'], s['nearest'],s['time_sec']) for s in nearest_stops]
pop = [ (s['stop_id'], s['stop_boardings']) for s in stop_pop]

dot = product(nearest, pop)
matches = [ (f,g) for (f,g) in dot if f[0] == g[0] ]

out_coll = 'people_second_utility'
repo.dropPermanent(out_coll)
repo.createPermanent(out_coll)

for line in ['GLB', 'GLC', 'GLD', 'GLE']:
    total_usage = sum([pop for ((s,l,n,w),(i,pop)) in matches if l == line])
    for stop,pop,sec in [(s,p,w) for ((s,l,n,w),(i,p)) in matches if l == line]:
        everyone_else = total_usage - pop
        ppl_seconds = (everyone_else * STOP_TIME) - (pop * sec)
        # now insert the people second utility into our data set.
        elements = {'stop':stop, 'ppl-secs':ppl_seconds, 'line': line}
        print(elements)
        repo['{}.{}'.format(teamname, out_coll)].insert_one(elements)

endTime = datetime.datetime.now()

# TODO provenance data

repo.logout()
