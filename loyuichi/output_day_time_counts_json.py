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

# Processing tickets to aggregate them by the day of the week and time they occur
daytimes = {"Sunday": [0]*24, "Monday": [0]*24, "Tuesday": [0]*24, "Wednesday": [0]*24, "Thursday": [0]*24, "Friday": [0]*24, "Saturday": [0]*24}
for ticket in repo['loyuichi.tickets'].find({"issue_time": {'$exists': True}, "issue_date": {'$exists': True}}):
	d_format = '%Y-%m-%dT%I:%M:%S %p'
	issue_datetime = datetime.datetime.strptime(ticket["issue_datetime"], d_format)
	hour = issue_datetime.hour
	day_week = issue_datetime.strftime('%A')
	print(issue_datetime.strftime(d_format))
	print(hour)
	print(day_week)
	daytimes[day_week][hour] += 1

# Outputting the results to a JSON file formatted for heatmap.html
days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
with open('daytime_counts_tickets.json', 'w') as outfile:
	out = ""
	out += json.dumps(daytimes)
	outfile.write(out)

# Processing tickets to aggregate them by the day of the week and time they occur
daytimes = {"Sunday": [0]*24, "Monday": [0]*24, "Tuesday": [0]*24, "Wednesday": [0]*24, "Thursday": [0]*24, "Friday": [0]*24, "Saturday": [0]*24}
for towed in repo['loyuichi.towed'].find({"fromdate": {'$exists': True}}):
	d_format = '%Y-%m-%dT%H:%M:%S'
	issue_datetime = datetime.datetime.strptime(towed["fromdate"], d_format)
	hour = issue_datetime.hour
	day_week = issue_datetime.strftime('%A')
	daytimes[day_week][hour] += 1

# Outputting the results to a JSON file formatted for heatmap.html
with open('daytime_counts_towed.json', 'w') as outfile:
	out = ""
	out += json.dumps(daytimes)
	outfile.write(out)

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

this_script = doc.agent('alg:output_day_time_counts', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

tickets = doc.entity('dat:tickets', {'prov:label':'Tickets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

aggr_datetime_tickets = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Aggregate Issue Datetime Tickets', prov.model.PROV_TYPE:'ont:Computation'})

doc.wasAssociatedWith(aggr_datetime_tickets, this_script)

doc.used(aggr_datetime_tickets, tickets, startTime)

#repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
#open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()

## eof


