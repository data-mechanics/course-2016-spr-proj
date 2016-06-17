'''
File:	extract_zip_info.py
Date:	02/24/16

This file is to extract zip and corresponding longitudes and 
latitudes information from approved_building_permits dataset 
in mongodb.
'''

import json
import datetime
import dml
import uuid
import provenance
import prov
import sys

#auth
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('linshan_luoty','linshan_luoty')
auth = json.loads(open(sys.argv[1]).read())

# Set up the database connection.
db = repo['linshan_luoty'+'.'+'approved_building_permits']

# Retrieve some data sets.
startTime = datetime.datetime.now()

# extract aipcode and (longitude, latitude) 
zips_locations = []
zl_arr = []
for document in db.find():
	if 'location' in document and 'zip' in document:
		zipcode = document['zip']
		longitude 	= document['location']['longitude']
		i = longitude.find('.')
		if i != -1:
			longitude = longitude[:i+4]	# rounded to three decimals
		latitude 	= document['location']['latitude']
		i = latitude.find('.')
		if i != -1:
			latitude = latitude[:i+4]	# rounded to three decimals
		zl = {
			'zip': 			zipcode,
			'longitude':	float(longitude),
			'latitude':		float(latitude)
		}

		zips_locations.append(zl)

# save it to a temporary folder
repo.dropTemporary("zips_locations")
repo.createTemporary("zips_locations")
repo['linshan_luoty.zips_locations'].insert_many(zips_locations)

endTime = datetime.datetime.now()

startTime = None
endTime = None

# Create the provenance document describing everything happening
# in this script. Each run of the script will generate a new
# document describing that invocation event. This information
# can then be used on subsequent runs to determine dependencies
# and "replay" everything. The old documents will also act as a
# log.

'''
doc = provenance.init()

this_script = doc.agent('alg:retrieve_datasets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

building = doc.entity('dat:approved_building_permits', {prov.model.PROV_LABEL:'Approved Building Permits', prov.model.PROV_TYPE:'ont:DataSet'})

get_zip_location = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL: "Extract zips and location info"})
doc.wasAssociatedWith(get_zip_location, this_script)
doc.usage(get_zip_location, building, startTime, None,
        {prov.model.PROV_TYPE:'ont:Computation'
        }
    )

zip_location = doc.entity('dat:zips_locations', {prov.model.PROV_LABEL:'Zips Locations', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(zip_location, this_script)
doc.wasGeneratedBy(zip_location, get_zip_location, endTime)
doc.wasDerivedFrom(zip_location, building, get_zip_location, get_zip_location, get_zip_location)

repo.record(doc.serialize()) # Record the provenance document.
provenance.update(doc)

print(doc.get_provn())
'''

repo.logout()
