'''
File:	get_total_crimes.py
Date:	04/12/16

This file is to 
1. aggregate crime_zips dataset in terms of zips and 
calculate the total number of crimes for each zip;
2. calculate the mean location of each zip.
'''

import json
import datetime
import pymongo
import prov.model
import provenance
import uuid
from bson.code import Code
from bson.json_util import dumps

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())
exec(open('get_repo.py').read())

# Set up the database connection.
db = repo[auth['admin']['name']+'.'+'crime_zips']


startTime = datetime.datetime.now()

# calculate the total lng,lat and total number of crimes for each zip
pipeline = [{"$group": {
				"_id": "$zip", 
				"total_lng": {"$sum": "$location.longitude"}, 
				"total_lat": {"$sum": "$location.latitude"}, 
				"total_crimes": {"$sum": 1}
				}
			}]

zip_location_crimes = list(db.aggregate(pipeline))

# calculate mean location
# zip_location_crimes = [{
# 						'zip': d['_id'], 
# 						'total_crimes': d['total_crimes'],
# 						'location': { 'longitude': d['total_lng']/d['total_crimes'],
# 									  'latitude':  d['total_lat']/d['total_crimes']} 
# 						} for d in zip_location_crimes ]
zip_location_crimes = [{
						'zip': d['_id'], 
						'crimes': d['total_crimes'],
						'longitude': d['total_lng']/d['total_crimes'],
						'latitude': d['total_lat']/d['total_crimes'],
						'region': d['total_crimes'],
						} for d in zip_location_crimes ]

# export zip_location_crimes to JSON
f = open('zip_location_crimes.json','w')
f.write(json.dumps(zip_location_crimes, indent=4))
f.close()

# save it to a temporary folder
repo.dropPermanent("zip_location_crimes")
repo.createPermanent("zip_location_crimes")
repo['linshan_luoty.zip_location_crimes'].insert_many(zip_location_crimes)



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

this_script = doc.agent('alg:get_total_crimes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

crime_zips = doc.entity('dat:crime_zips', {prov.model.PROV_LABEL:'Crime Zips', prov.model.PROV_TYPE:'ont:DataSet'})

get_total_crimes = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime)
doc.wasAssociatedWith(get_total_crimes, this_script)
doc.usage(get_total_crimes, crime_zips, startTime, None,
        {prov.model.PROV_TYPE:'ont:Computation'
        }
    )

zip_location_crimes = doc.entity('dat:zip_location_crimes', {prov.model.PROV_LABEL:'Zip Average Earnings', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(zip_location_crimes, this_script)
doc.wasGeneratedBy(zip_location_crimes, get_total_crimes, endTime)
doc.wasDerivedFrom(zip_location_crimes, crime_zips, get_total_crimes, get_total_crimes, get_total_crimes)

repo.record(doc.serialize()) # Record the provenance document.
provenance.update(doc)
print(doc.get_provn())

repo.logout()