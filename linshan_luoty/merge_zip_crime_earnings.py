import datetime
import pymongo
import sys

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
	
repo.logout()
