import urllib.request
import json
import datetime
import pymongo
import sys

datasets = {
	'crime_incident_reports':'https://data.cityofboston.gov/resource/7cdf-6fgx.json',
	'employee_earnings_report_2014':'https://data.cityofboston.gov/resource/4swk-wcg8.json',
	'approved_building_permits':'https://data.cityofboston.gov/resource/msk6-43c6.json'
}

# get credential file
if len(sys.argv) < 2:	# no auth.json is given
	auth_file = input('Please enter the auth file: ')
else:
	auth_file = sys.argv[1]
auth = json.loads(open(auth_file).read())

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate(auth['admin']['name'], auth['admin']['pwd'])

# Retrieve some data sets.
startTime = datetime.datetime.now()

for title in datasets:
	url = datasets[title]
	response = urllib.request.urlopen(url).read().decode("utf-8")
	r = json.loads(response)
	#s = json.dumps(r, sort_keys=True, indent=2)
	repo.dropPermanent(title)
	repo.createPermanent(title)
	repo[auth['admin']['name']+'.'+title].insert_many(r)

endTime = datetime.datetime.now()
