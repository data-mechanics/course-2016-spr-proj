import datetime
import pymongo
import sys

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
	

repo.logout()
