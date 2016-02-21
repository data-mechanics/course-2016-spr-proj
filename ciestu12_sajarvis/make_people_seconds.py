"""
Use the stop boarding counts and nearest stop datasets to come up with a
measure of utility provided by each stop, called 'people seconds'. This
measure can be used to determine the value of each stop.

Requires the collections:
    'nearest_stops'
    'boarding_counts'

Lower scores indicate a higher value, considering the overall time saved for
commuters.
"""
import json
import pymongo
import prov.model
import time
import datetime
import uuid

# Estimated time of the time cost (seconds) for stopping at a T stop.
STOP_TIME = 60

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

teamname = 'ciestu12_sajarvis'
# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate(teamname, teamname)
out_coll = 'people_second_utility'
repo.dropPermanent(out_coll)
repo.createPermanent(out_coll)

startTime = datetime.datetime.now()

def product(R, S):
    return [(t,u) for t in R for u in S]

# get DB for population and nearest stop
stop_pop = repo['{}.{}'.format(teamname, 'boarding_counts')].find({})
nearest_stops = repo['{}.{}'.format(teamname, 'nearest_stops')].find({})

# construct some tuples of the information we need
nearest = [(s['stop'], s['line'], s['nearest'], s['time_sec']) for s in nearest_stops]
pop = [(s['stop_id'], s['stop_boardings']) for s in stop_pop]

dot = product(nearest, pop)
matches = [(f,g) for (f,g) in dot if f[0] == g[0]]

for line in ['GLB', 'GLC', 'GLD', 'GLE']:
    total_usage = sum([pop for ((s,l,n,w),(i,pop)) in matches if l == line])
    for stop,pop,sec in [(s,p,w) for ((s,l,n,w),(i,p)) in matches if l == line]:
        everyone_else = total_usage - pop
        # the actual measure of utility. low scores are best.
        ppl_seconds = (everyone_else * STOP_TIME) - (pop * sec)
        # now insert the people second utility into our data set.
        elements = {'stop':stop, 'ppl-secs':ppl_seconds, 'line': line}
        print(elements)
        repo['{}.{}'.format(teamname, out_coll)].insert_one(elements)

endTime = datetime.datetime.now()

# TODO provenance data

repo.logout()
