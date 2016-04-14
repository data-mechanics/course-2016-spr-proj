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
repo.authenticate('enze', 'enze')

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

# Retrieve these data sets automatically

# Database 1: Liquor Licenses
# Filter out the bars
# url = 'https://data.cityofboston.gov/resource/hda6-fnsh.json?'
url = 'https://data.cityofboston.gov/resource/hda6-fnsh.json?liccat=CV7AL&city=Boston'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("liquor_license")
repo.createPermanent("liquor_license")
repo['enze.liquor_license'].insert_many(r)

# Database 2: Crime Incident Reports
url = 'https://data.cityofboston.gov/resource/7cdf-6fgx.json?'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("crime_report")
repo.createPermanent("crime_report")
repo['enze.crime_report'].insert_many(r)

# Count how many crime incidents happened in a given street which also contains bars.
# repo['enze.crime_report'].mapReduce(
# 	function() { emit(this.streetname, 1); }, 
# 	function(key, values) { return Array.sum(values) }, 
# 	{ 
# 		query: { year: 2015, streetname: {$in: repo['enze.liquor_license'].address} }, 
# 		out: "crime_totals" 
# 	}
# )

# Database 3: Waze Jam Data
url = 'https://data.cityofboston.gov/resource/yqgx-2ktq.json?city=Boston,%20MA'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("waze_boston")
repo.createPermanent("waze_boston")
repo['enze.waze_boston'].insert_many(r)

# Count how many road incidents happened in a given street which also contains bars.
# repo['enze.waze_boston'].mapReduce(
# 	function() { emit(this.street, 1); }, 
# 	function(key, values) { return Array.sum(values) }, 
# 	{ 
# 		query: { street: {$in: repo['enze.liquor_license'].address} }, 
# 		out: "waze_totals" 
# 	}
# )

endTime = datetime.datetime.now()

# Create the provenance document describing everything happening
# in this script. Each run of the script will generate a new
# document describing that invocation event. This information
# can then be used on subsequent runs to determine dependencies
# and "replay" everything. The old documents will also act as a
# log.
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'https://github.com/andrewenze/course-2016-spr-proj-one/enze/data_process.py') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'https://github.com/andrewenze/course-2016-spr-proj-one/enze/data') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'https://github.com/andrewenze/course-2016-spr-proj-one/enze/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'https://github.com/andrewenze/course-2016-spr-proj-one/enze/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

initial_script = doc.agent('alg:initialize', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

resource_liquor = doc.entity('bdp:hda6-fnsh', {'prov:label':'Liquor Licenses', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
resource_crime = doc.entity('bdp:7cdf-6fgx', {'prov:label':'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
resource_waze = doc.entity('bdp:yqgx-2ktq', {'prov:label':'Waze Jam Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

get_crime = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?year=2015&$select=streetname,day_week,location'})
get_waze = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?$select=street'})
doc.wasAssociatedWith(get_crime, initial_script)
doc.wasAssociatedWith(get_waze, initial_script)
doc.used(get_crime, resource_crime, startTime)
doc.used(get_waze, resource_waze, startTime)

crime_totals = doc.entity('dat:crime_totals', {prov.model.PROV_LABEL:'Crime Around Bars', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(crime_totals, initial_script)
doc.wasGeneratedBy(crime_totals, get_crime, endTime)
doc.wasDerivedFrom(crime_totals, resource_crime, resource_liquor)

waze_totals = doc.entity('dat:waze_totals', {prov.model.PROV_LABEL:'Jam Around Bars', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(waze_totals, initial_script)
doc.wasGeneratedBy(waze_totals, get_waze, endTime)
doc.wasDerivedFrom(waze_totals, resource_waze, resource_liquor)

repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()

## eof
