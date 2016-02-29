import urllib.request
import json
import datetime
import pymongo
import sys

datasets = {
	'crime_incident_reports':'https://data.cityofboston.gov/resource/7cdf-6fgx.json?year=2014&$limit=10000',
	'employee_earnings_report_2014':'https://data.cityofboston.gov/resource/4swk-wcg8.json?$limit=10000',
	'approved_building_permits':'https://data.cityofboston.gov/resource/msk6-43c6.json?$limit=10000'
}


# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())
exec(open('get_repo.py').read())

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


repo.logout()
