import datetime
import pymongo
import sys

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())
exec(open('get_repo.py').read())

crime_db = repo[auth['admin']['name']+'.'+'crime_incident_reports']
zip_db	 = repo[auth['admin']['name']+'.'+'zips_locations']

startTime = datetime.datetime.now()

# go through every entry in crime_incident_reports, and associate it with a zipcode.
crime_zips = []
project_set = {'_id': False, 'shooting': True, 'day_week': True, 'fromdate': True, 'location': True}
for document in crime_db.find({}, project_set):
	zipcode = zip_db.find_one({'longitude': document['location']['longitude'],
								 'latitude': document['location']['latitude']}, {'_id': False, 'zip': True})
	if zipcode is None: 
		continue
	else:
		document['zip'] = zipcode['zip']
		crime_zips.append(document)

# save it to a temporary folder
repo.dropTemporary("crime_zips")
repo.createTemporary("crime_zips")
repo['linshan_luoty.crime_zips'].insert_many(crime_zips)

endTime = datetime.datetime.now()
	

repo.logout()
