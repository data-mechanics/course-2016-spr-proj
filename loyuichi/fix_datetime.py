from geopy.geocoders import GoogleV3
import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('loyuichi', 'loyuichi')

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

# Combining the issue_date and issue_time fields to create a complete datetime field for tickets
for ticket in repo['loyuichi.tickets'].find({"issue_time": {'$exists': True}, "issue_date": {'$exists': True}}):
	issue_date = ticket['issue_date']
	issue_time = ticket['issue_time']
	issue_datetime = issue_date.split('T')[0] + "T" + issue_time
	d_format = '%Y-%m-%dT%I:%M:%S %p'
	d = datetime.datetime.strptime(issue_datetime, d_format)

	res = repo['loyuichi.tickets'].update({'_id': ticket['_id']}, {'$set': {'issue_datetime': d.strftime(d_format)}})
	print(res)

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

this_script = doc.agent('alg:fix_datetime', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

tickets = doc.entity('dat:tickets', {'prov:label':'Tickets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

fix_datetime_tickets = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Fix Issue Datetime Tickets', prov.model.PROV_TYPE:'ont:Computation'})

doc.wasAssociatedWith(fix_datetime_tickets, this_script)

doc.used(fix_datetime_tickets, tickets, startTime)

db_tickets = doc.entity('dat:tickets', {prov.model.PROV_LABEL:'Parking Tickets', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_tickets, this_script)
doc.wasGeneratedBy(db_tickets, fix_datetime_tickets, endTime)
doc.wasDerivedFrom(db_tickets, tickets, fix_datetime_tickets, fix_datetime_tickets, fix_datetime_tickets)

#repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
#open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()

## eof


