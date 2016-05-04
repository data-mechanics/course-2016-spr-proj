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

# Process through Zip Stats collection to record all zip code counts per category
data = {"food_est_counts": {}, "meter_counts": {}, "towed_counts": {}, "tickets_counts": {}}
for stat in repo['loyuichi.zip_stats'].find():
	zipcode = stat["zip"]
	data["food_est_counts"].update({zipcode: stat["food_establishments"]})
	data["meter_counts"].update({zipcode: stat["meters"]})
	data["towed_counts"].update({zipcode: stat["towed"]})
	data["tickets_counts"].update({zipcode: stat["tickets"]})

# Write processed Zip Stats to json file for zipcode.html to read
labels = ["food_est_counts", "meter_counts", "towed_counts", "tickets_counts"]
with open('counts.json', 'w') as outfile:
	out = ""
	for i in range(len(labels)):
		out += "var " + labels[i] + " = "
		out += json.dumps(data[labels[i]])
		out += "\n"
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

this_script = doc.agent('alg:output_counts_json', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

zip_stats = doc.entity('dat:zip_stats', {'prov:label':'Zip Stats', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

convert_zip_stats = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Address Meters', prov.model.PROV_TYPE:'ont:Computation'})

doc.wasAssociatedWith(convert_zip_stats, this_script)

doc.used(convert_zip_stats, zip_stats, startTime)

#repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
#open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()

## eof


