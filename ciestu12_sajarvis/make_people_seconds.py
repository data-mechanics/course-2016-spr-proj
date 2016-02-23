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

# Create provenance data and recording
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ciestu12_sajarvis/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/ciestu12_sajarvis/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.

this_script = doc.agent('alg:make_people_seconds', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
# Other input resources
nearest_resource = doc.entity('dat:nearest_stops', {'prov:label':'Green Line Nearest Stops', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
boarding_resource = doc.entity('dat:boarding_counts', {'prov:label':'Green Line Boarding Counts', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':''})
doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, nearest_resource, startTime)
doc.used(this_run, boarding_resource, startTime)

ppl_seconds = doc.entity('dat:people_second_utility', {prov.model.PROV_LABEL:'Measure of People Seconds Utility', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(ppl_seconds, this_script)
doc.wasGeneratedBy(ppl_seconds, this_run, endTime)
doc.wasDerivedFrom(ppl_seconds, nearest_resource, this_run, this_run, this_run)
doc.wasDerivedFrom(ppl_seconds, boarding_resource, this_run, this_run, this_run)

repo.record(doc.serialize()) # Record the provenance document.
print(doc.get_provn())

repo.logout()
