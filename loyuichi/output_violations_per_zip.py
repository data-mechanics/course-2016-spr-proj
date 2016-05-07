import urllib.request
import pymongo
import prov.model
import datetime
import uuid
import csv

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('loyuichi', 'loyuichi')

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

violation_labels = ["HP-DV PLATE: $120",
"HANDICAP RAMP: $100",
"HYDRANT: $100",
"B - BUS STOP/STAND: $100",
"LESS 10FT FIRELANE: $100",
"CROSSWALK: $85",
"NO STOP OR STAND: $75",
"SIDEWALK: $65",
"LOADING ZONE: $55",
"NO PARKING: $55",
"TAXI STAND: $50",
"DOUBLE PARK ZONE A: $45",
"EXPIRED/NO PLATE: $40",
"EXPIRED INSPECTION: $40",
"RESIDENT PARK ONLY: $40",
"W/IN 20 FT INTERSECT: $40",
"OVER 1FT FROM CURB: $35",
"DOUBLE PKG-ZONE B: $30",
"OVER METER LIMIT: $25",
"OVER POST LIM-ZONE B: $25",
"METER FEE UNPAID: $25",
"OVER POSTED LIMIT: $25",
"NO PARKING-ZONE B: $25",
"OTHER: $15"]

violations = {
	"HP-DV PLATE": 0,
	"HANDICAP RAMP": 1,
	"HYDRANT": 2,
	"B - BUS STOP/STAND": 3,
	"LESS 10FT FIRELANE": 4,
	"CROSSWALK": 5,
	"NO STOP OR STAND": 6,
	"SIDEWALK": 7,
	"LOADING ZONE": 8,
	"NO PARKING": 9,
	"TAXI STAND": 10,
	"DOUBLE PARK ZONE A": 11,
	"EXPIRED/NO PLATE": 12,
	"EXPIRED INSPECTION": 13,
	"RESIDENT PARK ONLY": 14,
	"W/IN 20 FT INTERSECT": 15,
	"OVER 1FT FROM CURB": 16,
	"DOUBLE PKG-ZONE B": 17,
	"OVER METER LIMIT": 18,
	"OVER POST LIM-ZONE B": 19,
	"METER FEE UNPAID": 20,
	"OVER POSTED LIMIT": 21,
	"NO PARKING-ZONE B": 22,
	"OTHER": 23
}

# Processing tickets to aggregate them by the day of the week and time they occur
zipcode_tickets = []
for item in repo['loyuichi.tickets'].aggregate([{'$group': {'_id': {'zip': "$zip", 'violation': '$violation1'}, 'count': { '$sum': 1 }}}, {'$group': {'_id': '$_id.zip', 'tickets': { '$push': {'violation': '$_id.violation', 'count': '$count'},},'count': {'$sum': '$count'}}},{'$sort': {'count': -1}}]):
	if (item['_id'] is None or item['_id'] == 'United States'):
		pass
	else:
		current = [item['_id']] + [0]*len(violations)
		for violation in item['tickets']:
			current[1 + violations[violation['violation']]] = violation['count']
		zipcode_tickets += [current]

# Outputting the results to a JSON file formatted for heatmap.html
days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
with open('violations_per_zip.csv', 'w') as outfile:
	writer = csv.writer(outfile)
	writer.writerow(['zip'] + violation_labels)
	writer.writerows(zipcode_tickets)

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

this_script = doc.agent('alg:output_violations_per_zip', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

tickets = doc.entity('dat:tickets', {'prov:label':'Tickets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

aggr_zipcode_tickets_violations = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Aggregate Zip Code Tickets Violations', prov.model.PROV_TYPE:'ont:Computation'})

doc.wasAssociatedWith(aggr_zipcode_tickets_violations, this_script)

doc.used(aggr_zipcode_tickets_violations, tickets, startTime)

#repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
#open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()

## eof



