import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid
import datetime

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('loyuichi', 'loyuichi')

# Methods from CS591 course notes
def union(lists):
    union_list = lists[0]
    for l in lists[1:]:
    	union_list += [v for v in l if v not in union_list]
    return sorted(union_list)

def difference(R, S):
    return [t for t in R if t not in S]

def intersect(R, S):
    return [t for t in R if t in S]

def project(R, p):
    return [p(t) for t in R]

def select(R, s):
    return [t for t in R if s(t)]
 
def product(R, S):
    return [(t,u) for t in R for u in S]

def aggregate(R, field, f):
    keys = {r[field] for r in R}
    #return [(key, f([v for vals in R if vals[field] == key] + [])) for key in keys]
    return [{"day_time": key, "violations": [f(vals) for vals in R if vals[field] == key] + []} for key in keys]

def map(f, R):
    return [t for (k,v) in R for t in f(k,v)]
    
def reduce(f, R):
    keys = {k for (k,v) in R}
    return [f(k1, [v for (k2,v) in R if k1 == k2]) for k1 in keys]

def addDateInfo(R):
	for doc in R:
		if "issue_date" in doc:
			d = datetime.datetime.strptime(doc["issue_date"], '%Y-%m-%dT%H:%M:%S')
			doc["day_week"] = d.strftime('%A')
			doc["time_day"] = d.strftime('%H')
			doc["day_time"] = d.strftime('%A %H')
		else:
			d = datetime.datetime.strptime(doc["fromdate"], '%Y-%m-%dT%H:%M:%S')
			doc["time_day"] = d.strftime('%H')
			doc["day_time"] = d.strftime('%A %H')

	return R

def extractKeyInfo(vals):
	if "fromdate" in vals:
		return {"_id": vals["_id"], "violation1": "TOWED"}
	else:
		return {"_id": vals["_id"], "violation1": vals["violation1"]}

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

# # Aggregate street names across all four datasets
# repo.dropPermanent('streetnames')
# repo.createPermanent('streetnames')
# streetnames_meters = repo['loyuichi.meters'].distinct("streetname")
# streetnames_towed = repo['loyuichi.towed'].distinct("streetname")
# streetnames_tickets = repo['loyuichi.tickets'].distinct("streetname")
# streetnames_food_establishments = repo['loyuichi.food_establishments'].distinct("streetname")

# streetnames = union([streetnames_tickets, streetnames_towed, streetnames_tickets, streetnames_food_establishments])
# repo['loyuichi.streetnames'].insert({"names": streetnames})

# Aggregate meters, tickets, tows, food establishments by street name
# repo.dropPermanent('street_stats')
# repo.createPermanent('street_stats')
# for street in streetnames:
# 	values = {"streetname": street}
# 	values["meters"] = repo['loyuichi.meters'].find({"streetname": street}).count()
# 	values["tickets"] = repo['loyuichi.tickets'].find({"streetname": street}).count()
# 	values["towed"] = repo['loyuichi.towed'].find({"streetname": street}).count()
# 	values["food_establishments"] = repo['loyuichi.food_establishments'].find({"streetname": street}).count()
# 	repo['loyuichi.street_stats'].insert(values)

# Aggregate tickets and tows according to day of the week and hour
repo.dropPermanent('day_violations')
repo.createPermanent('day_violations')
retrieve_tickets = repo['loyuichi.tickets'].find({"issue_time": {'$exists': True}, "issue_date": {'$exists': True}}, {"issue_time": 1, "issue_date": 1, "violation1": 1})
retrieve_towed = repo['loyuichi.towed'].find({"day_week": {'$exists': True}, "fromdate": {'$exists': True}}, {"day_week": 1, "fromdate": 1})
modified_tickets = addDateInfo(list(retrieve_tickets))
modified_towed = addDateInfo(list(retrieve_towed))
grouped_by_day_time = aggregate(modified_tickets + modified_towed, "day_time", extractKeyInfo)
repo['loyuichi.day_violations'].insert_many(grouped_by_day_time)

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

this_script = doc.agent('alg:merge', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

towed = doc.entity('dat:towed', {'prov:label':'Towed', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
food_establishments = doc.entity('dat:food_establishments', {'prov:label':'Food Establishment Permits', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
tickets = doc.entity('dat:tickets', {'prov:label':'Parking Tickets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
meters = doc.entity('dat:meters', {'prov:label':'Parking Meters', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

merge_streetnames = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime)
merge_street_stats = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime)
count_towed = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Count Towed', prov.model.PROV_TYPE:'ont:Computation'})
count_meters = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Count Meters', prov.model.PROV_TYPE:'ont:Computation'})
count_tickets = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Count Tickets', prov.model.PROV_TYPE:'ont:Computation'})
count_food_establishments = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Count Food Establishments', prov.model.PROV_TYPE:'ont:Computation'})
add_date_time_tickets = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Add Date and Time to Tickets', prov.model.PROV_TYPE:'ont:Computation'})
add_date_time_towed = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Add Date and Time to Towed', prov.model.PROV_TYPE:'ont:Computation'})
aggregate_tickets_towed = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Add Date and Time to Towed', prov.model.PROV_TYPE:'ont:Computation'})

doc.wasAssociatedWith(merge_streetnames, this_script)
doc.wasAssociatedWith(merge_street_stats, this_script)
doc.wasAssociatedWith(count_towed, this_script)
doc.wasAssociatedWith(count_meters, this_script)
doc.wasAssociatedWith(count_tickets, this_script)
doc.wasAssociatedWith(count_food_establishments, this_script)
doc.wasAssociatedWith(add_date_time_tickets, this_script)
doc.wasAssociatedWith(add_date_time_towed, this_script)
doc.wasAssociatedWith(aggregate_tickets_towed, this_script)

doc.used(merge_streetnames, towed, startTime)
doc.used(merge_streetnames, food_establishments, startTime)
doc.used(merge_streetnames, tickets, startTime)
doc.used(merge_streetnames, meters, startTime)
doc.used(merge_street_stats, towed, startTime)
doc.used(merge_street_stats, food_establishments, startTime)
doc.used(merge_street_stats, tickets, startTime)
doc.used(merge_street_stats, meters, startTime)
doc.used(count_towed, towed, startTime)
doc.used(count_food_establishments, food_establishments, startTime)
doc.used(count_tickets, tickets, startTime)
doc.used(count_meters, meters, startTime)
doc.used(add_date_time_tickets, tickets, startTime)
doc.used(add_date_time_towed, towed, startTime)
doc.used(aggregate_tickets_towed, towed, startTime)

db_towed = doc.entity('dat:towed', {prov.model.PROV_LABEL:'Towed', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_towed, this_script)
doc.wasGeneratedBy(db_towed, count_towed, endTime)
doc.wasDerivedFrom(db_towed, towed, count_towed, count_towed, count_towed)

db_food_establishments = doc.entity('dat:food_establishments', {prov.model.PROV_LABEL:'Food Establishments Permits', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_food_establishments, this_script)
doc.wasGeneratedBy(db_food_establishments, count_food_establishments, endTime)
doc.wasDerivedFrom(db_food_establishments, food_establishments, count_food_establishments, count_food_establishments, count_food_establishments)

db_tickets = doc.entity('dat:tickets', {prov.model.PROV_LABEL:'Parking Tickets', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_tickets, this_script)
doc.wasGeneratedBy(db_tickets, count_tickets, endTime)
doc.wasDerivedFrom(db_tickets, tickets, count_tickets, count_tickets, count_tickets)

db_meters = doc.entity('dat:meters', {prov.model.PROV_LABEL:'Parking Meters', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_meters, this_script)
doc.wasGeneratedBy(db_meters, count_meters, endTime)
doc.wasDerivedFrom(db_meters, meters, count_meters, count_meters, count_meters)

db_streetnames = doc.entity('dat:streetnames', {prov.model.PROV_LABEL:'Street Names', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_streetnames, this_script)
doc.wasGeneratedBy(db_streetnames, merge_streetnames, endTime)
doc.wasDerivedFrom(db_streetnames, towed, merge_streetnames, merge_streetnames, merge_streetnames)
doc.wasDerivedFrom(db_streetnames, meters, merge_streetnames, merge_streetnames, merge_streetnames)
doc.wasDerivedFrom(db_streetnames, food_establishments, merge_streetnames, merge_streetnames, merge_streetnames)
doc.wasDerivedFrom(db_streetnames, tickets, merge_streetnames, merge_streetnames, merge_streetnames)

db_street_stats = doc.entity('dat:street_stats', {prov.model.PROV_LABEL:'Street Stats', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_street_stats, this_script)
doc.wasGeneratedBy(db_street_stats, merge_street_stats, endTime)
doc.wasDerivedFrom(db_street_stats, towed, merge_street_stats, merge_street_stats, merge_street_stats)
doc.wasDerivedFrom(db_street_stats, meters, merge_street_stats, merge_street_stats, merge_street_stats)
doc.wasDerivedFrom(db_street_stats, food_establishments, merge_street_stats, merge_street_stats, merge_street_stats)
doc.wasDerivedFrom(db_street_stats, tickets, merge_street_stats, merge_street_stats, merge_street_stats)

db_day_violations = doc.entity('dat:day_violations', {prov.model.PROV_LABEL:'Violations by Day', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_day_violations, this_script)
doc.wasGeneratedBy(db_day_violations, aggregate_tickets_towed, endTime)
doc.wasDerivedFrom(db_day_violations, towed, aggregate_tickets_towed, aggregate_tickets_towed, aggregate_tickets_towed)
doc.wasDerivedFrom(db_day_violations, tickets, aggregate_tickets_towed, aggregate_tickets_towed, aggregate_tickets_towed)

#repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
#open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()

## eof
