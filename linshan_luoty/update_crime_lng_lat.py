import json
import datetime
import pymongo
import prov.model
import uuid

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())
exec(open('get_repo.py').read())

# Set up the database connection.
db = repo[auth['admin']['name']+'.'+'crime_incident_reports']

startTime = datetime.datetime.now()

# update crime dataset
for document in db.find():
	if 'location' in document:
		longitude 	= document['location']['longitude']
		i = longitude.find('.')
		if i != -1:
			longitude = longitude[:i+4]	# rounded to three decimals
		latitude 	= document['location']['latitude']
		i = latitude.find('.')
		if i != -1:
			latitude = latitude[:i+4]	# rounded to three decimals
		db.update_one({'_id':document['_id']}, {'$set': {'location': {'longitude': longitude, 'latitude': latitude}}})

endTime = datetime.datetime.now()
	

# Create the provenance document describing everything happening
# in this script. Each run of the script will generate a new
# document describing that invocation event. This information
# can then be used on subsequent runs to determine dependencies
# and "replay" everything. The old documents will also act as a
# log.
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'https://data-mechanics.s3.amazonaws.com/linshan_luoty/algorithm/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'https://data-mechanics.s3.amazonaws.com/linshan_luoty/data/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'https://data-mechanics.s3.amazonaws.com/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'https://data-mechanics.s3.amazonaws.com/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:update_crime_lng_lat', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

crime = doc.entity('dat:crime_incident_reports', {prov.model.PROV_LABEL:'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataSet'})

update_location = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime)
doc.wasAssociatedWith(update_location, this_script)
doc.usage(update_location, crime, startTime, None,
        {prov.model.PROV_TYPE:'ont:Computation'
        }
    )

doc.wasAttributedTo(crime, this_script)
doc.wasGeneratedBy(crime, update_location, endTime)
doc.wasDerivedFrom(crime, crime, update_location, update_location, update_location)

repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
open('plan.json','a').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())

repo.logout()
