import json
import datetime
import pymongo
import prov.model
import uuid

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())
exec(open('get_repo.py').read())

crime_zips_db		 = repo[auth['admin']['name']+'.'+'crime_zips']
zip_avg_earnings_db	 = repo[auth['admin']['name']+'.'+'zip_avg_earnings']

startTime = datetime.datetime.now()

# count the number of crime incidents for each zipcode
pipeline = [{"$group": {"_id": "$zip", "number_crime_incidents": {"$sum": 1}}}]

zip_crimes = list(crime_zips_db.aggregate(pipeline))
# replace '_id' with 'zip'
zip_crimes = [ {'zip': d['_id'], 'number_crime_incidents': d['number_crime_incidents']} for d in zip_crimes ]

zip_crime_earnings = []
for document in zip_crimes:
	avg_earning = zip_avg_earnings_db.find_one({'zip': document['zip']}, {'_id': False, 'avg_earning': True})
	if avg_earning is None: 
		document['avg_earning'] = 0
	else:
		document['avg_earning'] = avg_earning['avg_earning']
	zip_crime_earnings.append(document)

# save it to a temporary folder
repo.dropPermanent("zip_crime_earnings")
repo.createPermanent("zip_crime_earnings")
repo['linshan_luoty.zip_crime_earnings'].insert_many(zip_crime_earnings)

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

this_script = doc.agent('alg:merge_zip_crime_earnings', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

crime_zip = doc.entity('dat:crime_zips', {prov.model.PROV_LABEL:'Crime Zips', prov.model.PROV_TYPE:'ont:DataSet'})
zip_avg_earning = doc.entity('dat:zip_avg_earnings', {prov.model.PROV_LABEL:'Zips Average Earnings', prov.model.PROV_TYPE:'ont:DataSet'})

merge_zip_crime_earnings = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime)
doc.wasAssociatedWith(merge_zip_crime_earnings, this_script)
doc.usage(merge_zip_crime_earnings, crime_zip, startTime, None,
        {prov.model.PROV_TYPE:'ont:Computation'
        }
    )
doc.usage(merge_zip_crime_earnings, zip_avg_earning, startTime, None,
        {prov.model.PROV_TYPE:'ont:Computation'
        }
    )

zip_crime_earning = doc.entity('dat:zip_crime_earnings', {prov.model.PROV_LABEL:'Zips with Crime and Earnings', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(zip_crime_earning, this_script)
doc.wasGeneratedBy(zip_crime_earning, merge_zip_crime_earnings, endTime)
doc.wasDerivedFrom(zip_crime_earning, crime_zip, merge_zip_crime_earnings, merge_zip_crime_earnings, merge_zip_crime_earnings)
doc.wasDerivedFrom(zip_crime_earning, zip_avg_earning, merge_zip_crime_earnings, merge_zip_crime_earnings, merge_zip_crime_earnings)

repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
open('plan.json','a').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
	
repo.logout()
