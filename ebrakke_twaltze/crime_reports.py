import prov.model
import dml
import datetime
import urllib.request
import time
import uuid
import sys

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('ebrakke_twaltze', 'ebrakke_twaltze')

def crime_reports():
	auth = json.loads(open(sys.argv[1]).read())['services']['cityofbostondataportal']

	client = pymongo.MongoClient()
	repo = client.repo
	repo.authenticate('ebrakke_twaltze', 'ebrakke_twaltze')

	repo.dropPermanent('crimeReports')
	repo.createPermanent('crimeReports')

	offset = 0
	max_requests = 50000
	url = '{}resource/ufcx-3fdn.json?$limit={}&$offset={}&$$app_token={}'
	start_time = datetime.datetime.now()
	while True:
		this_request = url.format(auth['service'], max_requests, offset, auth['token'])
		print(this_request)
		response = urllib.request.urlopen(this_request).read().decode('utf-8')
		r = json.loads(response)
		if len(r) == 0:
			break
		repo['ebrakke_twaltze.crimeReports'].insert_many(r)
		offset += max_requests
		time.sleep(.5)
	end_time = datetime.datetime.now()

	do_prov(start_time, end_time)

def do_prov(start_time=None, end_time=None):
	doc = prov.model.ProvDocument()
	doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ebrakke_twaltze/')
	doc.add_namespace('dat', 'http://datamechanics.io/data/ebrakke_twaltze/')
	doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
	doc.add_namespace('log', 'http://datamechanics.io/log#')
	doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

	script = doc.agent('alg:crime_reports', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
	crime_reports = doc.entity('bdp:ufcx-3fdn', {prov.model.PROV_LABEL:'Crime Reports', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

	get_crime_reports = doc.activity('log:a' + str(uuid.uuid4()), start_time, end_time, {prov.model.PROV_LABEL: 'Fetch all crime reports'})
	doc.wasAssociatedWith(get_crime_reports, script)
	doc.usage(get_crime_reports, crime_reports, start_time, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

	crime = doc.entity('dat:crimeReports', {prov.model.PROV_LABEL:'Crime Reports', prov.model.PROV_TYPE:'ont:DataSet'})
	doc.wasAttributedTo(crime, script)
	doc.wasGeneratedBy(crime, get_crime_reports, end_time)
	doc.wasDerivedFrom(crime, crime_reports, get_crime_reports, get_crime_reports, get_crime_reports)

	repo.record(doc.serialize())
	print(doc.get_provn())

if __name__ == '__main__':
	if len(sys.argv) < 2:
		print('Please provide a path to the authorization file')
		exit(1)
	crime_reports()
