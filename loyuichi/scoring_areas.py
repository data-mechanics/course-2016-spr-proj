import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from random import shuffle
from math import sqrt

# Set up the database connection.
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('loyuichi', 'loyuichi')

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

# Calculating p-value based on CS591 notes
def avg(x): # Average
    return sum(x)/len(x)

def stddev(x): # Standard deviation.
    m = avg(x)
    return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

# Aggregate food establishment counts by zip code
fe = repo['loyuichi.food_establishments'].aggregate([{'$group': {'_id': "$zip", 'count': { '$sum': 1 }}}])
fe_zips = repo['loyuichi.food_establishments'].distinct("zip")

# Aggregate ticket counts by zip code
tickets = repo['loyuichi.tickets'].aggregate([{'$group': {'_id': "$zip", 'count': { '$sum': 1 }}}])
tickets_zips = repo['loyuichi.tickets'].distinct("zip")

# Converting cursor objects to dictionaries
tickets_by_zip = {}
for ticket in tickets:
	tickets_by_zip.update({ticket["_id"]: ticket["count"]})

fe_by_zip = {}
for f in fe:
	fe_by_zip.update({f["_id"]: f["count"]})
print(fe_by_zip)

towed = repo['loyuichi.towed'].aggregate([{'$group': {'_id': "$zip", 'count': { '$sum': 1 }}}])
towed_zips = repo['loyuichi.towed'].distinct("zip")

towed_by_zip = {}
for t in towed:
	towed_by_zip.update({t["_id"]: t["count"]})

meters = repo['loyuichi.meters'].aggregate([{'$group': {'_id': "$zip", 'count': { '$sum': 1 }}}])
meters_zips = repo['loyuichi.meters'].distinct("zip")

meters_by_zip = {}
for t in meters:
    meters_by_zip.update({t["_id"]: t["count"]})

zips = [fe_by_zip, tickets_by_zip, towed_by_zip, meters_by_zip]
zip_lengths = [("Meters", len(meters_by_zip), 3), ("Food Est", len(fe_by_zip), 0), ("Tickets", len(tickets_by_zip), 1), ("Towed", len(towed_by_zip), 2)]
min_zip_set = min(zip_lengths, key = lambda t: t[1])
print(min_zip_set)
print(min_zip_set[2])
print([zips[3][k] for k in zips[3] if k in zips[min_zip_set[2]]])
# Create (# of Food Establishment, # of Tickets) tuples based on zip codes for correlation analysis
means = [(avg([z[k] for k in z if k in zips[min_zip_set[2]]]), stddev([z[k] for k in z if k in zips[min_zip_set[2]]])) for z in zips]
normalized_fe_by_zip = {c: fe_by_zip[c]*1.0/max(fe_by_zip.values())*100 for c in fe_by_zip if c in zips[min_zip_set[2]]}
normalized_tickets_by_zip = {c: tickets_by_zip[c]*1.0/max(tickets_by_zip.values())*100 for c in tickets_by_zip if c in zips[min_zip_set[2]]}
normalized_towed_by_zip = {c: towed_by_zip[c]*1.0/max(towed_by_zip.values())*100 for c in towed_by_zip if c in zips[min_zip_set[2]]}
normalized_meters_by_zip = {c: meters_by_zip[c]*1.0/max(meters_by_zip.values())*100 for c in meters_by_zip if c in zips[min_zip_set[2]]}
print(normalized_meters_by_zip)

score = {}
for z in zips[min_zip_set[2]]:
    score[z] += normalized_fe_by_zip[z]
    score[z] += normalized_meters_by_zip[z]
    score[z] -= normalized_towed_by_zip[z]
    score[z] -= normalized_tickets_by_zip[z]

print(score)

endTime = datetime.datetime.now()

# Create the provenance document describing everything happening
# in this script. Each run of the script will generate a new
# document describing that invocation event. This information
# can then be used on subsequent runs to determine dependencies
# and "replay" everything. The old documents will also act as a
# log.
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/loyuichi/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/loyuichi/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:scoring areas', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

food_establishments = doc.entity('dat:food_establishments', {'prov:label':'Food Establishment Permits', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
towed = doc.entity('dat:towed', {'prov:label':'Towed', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
tickets = doc.entity('dat:tickets', {'prov:label':'Tickets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
meters = doc.entity('dat:meters', {'prov:label':'Meters', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

aggr_food_establishments = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Test Towed', prov.model.PROV_TYPE:'ont:Computation'})
aggr_tickets = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'aggr Tickets', prov.model.PROV_TYPE:'ont:Computation'})
calc_tickets = ddoc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'aggr Tickets', prov.model.PROV_TYPE:'ont:Computation'})

doc.wasAssociatedWith(aggr_food_establishments, this_script)
doc.wasAssociatedWith(aggr_tickets, this_script)
doc.wasAssociatedWith(calc_tickets, this_script)

doc.used(aggr_food_establishments, food_establishments, startTime)
doc.used(aggr_tickets, tickets_zips, startTime)
doc.used(aggr_towed, towed, startTime)
doc.used(calc_tickets, tickets, startTime)

#repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
#open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()

## eof
