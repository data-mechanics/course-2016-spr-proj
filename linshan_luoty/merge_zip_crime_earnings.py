import json
import datetime
import pymongo
import prov.model
import provenance
import uuid

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())
exec(open('get_repo.py').read())

zip_location_crimes_db	= repo[auth['admin']['name']+'.'+'zip_location_crimes']
zip_avg_earnings_db		= repo[auth['admin']['name']+'.'+'zip_avg_earnings']

startTime = datetime.datetime.now()

zip_location_crimes = zip_location_crimes_db.find({},{
	'_id': False,
	'zip': True,
	'crimes': True,
	'longitude': True,
	'latitude': True,
	'region': True,
	})

zip_location_crimes_earnings = []
for document in zip_location_crimes:
	avg_earning = zip_avg_earnings_db.find_one({'zip': document['zip']}, {'_id': False, 'avg_earning': True})
	if avg_earning is None: 
		document['avg_earning'] = 0
	else:
		document['avg_earning'] = avg_earning['avg_earning']
	zip_location_crimes_earnings.append(document)

# export zip_location_crimes_earnings to JSON
f = open('zip_location_crimes_earnings.json','w')
f.write(json.dumps(zip_location_crimes_earnings, indent=4))
f.close()

# save it to a permanent folder
repo.dropPermanent("zip_location_crimes_earnings")
repo.createPermanent("zip_location_crimes_earnings")
repo[auth['admin']['name']+'.'+'zip_location_crimes_earnings'].insert_many(zip_location_crimes_earnings)

zip_location_crimes_earnings_sorted = repo[auth['admin']['name']+'.'+'zip_location_crimes_earnings'].find({},{
	'_id': False,
	'zip': True,
	'crimes': True,
	'longitude': True,
	'latitude': True,
	'region': True,
	'avg_earning': True,
	}).sort([('avg_earning', pymongo.ASCENDING)])

f = open('zip_location_crimes_earnings_sorted.json','w')
f.write(json.dumps(list(zip_location_crimes_earnings_sorted), indent=4))
f.close()

endTime = datetime.datetime.now()


# Create the provenance document describing everything happening
# in this script. Each run of the script will generate a new
# document describing that invocation event. This information
# can then be used on subsequent runs to determine dependencies
# and "replay" everything. The old documents will also act as a
# log.
doc = provenance.init()
doc.add_namespace('alg', 'https://data-mechanics.s3.amazonaws.com/linshan_luoty/algorithm/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'https://data-mechanics.s3.amazonaws.com/linshan_luoty/data/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'https://data-mechanics.s3.amazonaws.com/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'https://data-mechanics.s3.amazonaws.com/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:merge_zip_crime_earnings', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

zip_location_crimes = doc.entity('dat:zip_location_crimes', {prov.model.PROV_LABEL:'Zip Location Crimes', prov.model.PROV_TYPE:'ont:DataSet'})
zip_avg_earning = doc.entity('dat:zip_avg_earnings', {prov.model.PROV_LABEL:'Zips Average Earnings', prov.model.PROV_TYPE:'ont:DataSet'})

merge_zip_crime_earnings = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime)
doc.wasAssociatedWith(merge_zip_crime_earnings, this_script)
doc.usage(merge_zip_crime_earnings, zip_location_crimes, startTime, None,
        {prov.model.PROV_TYPE:'ont:Computation'
        }
    )
doc.usage(merge_zip_crime_earnings, zip_avg_earning, startTime, None,
        {prov.model.PROV_TYPE:'ont:Computation'
        }
    )

zip_location_crimes_earnings = doc.entity('dat:zip_location_crimes_earnings', {prov.model.PROV_LABEL:'Zips with Crime and Earnings', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(zip_location_crimes_earnings, this_script)
doc.wasGeneratedBy(zip_location_crimes_earnings, merge_zip_crime_earnings, endTime)
doc.wasDerivedFrom(zip_location_crimes_earnings, zip_location_crimes, merge_zip_crime_earnings, merge_zip_crime_earnings, merge_zip_crime_earnings)
doc.wasDerivedFrom(zip_location_crimes_earnings, zip_avg_earning, merge_zip_crime_earnings, merge_zip_crime_earnings, merge_zip_crime_earnings)

repo.record(doc.serialize()) # Record the provenance document.
provenance.update(doc)
print(doc.get_provn())
	
repo.logout()
