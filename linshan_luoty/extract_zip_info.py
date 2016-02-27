'''
File:	extract_zip_info.py
Date:	02/24/16

This file is to extract zip and corresponding longitudes and 
latitudes information from approved_building_permits dataset 
in mongodb.
'''

import pymongo

exec(open('../pymongo_dm.py').read())
exec(open('get_repo.py').read())

# Set up the database connection.
db = repo[auth['admin']['name']+'.'+'approved_building_permits']

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
			'longitude':	longitude,
			'latitude':		latitude
		}

		zips_locations.append(zl)

# save it to a temporary folder
repo.dropTemporary("zips_locations")
repo.createTemporary("zips_locations")
repo['linshan_luoty.zips_locations'].insert_many(zips_locations)

repo.logout()
