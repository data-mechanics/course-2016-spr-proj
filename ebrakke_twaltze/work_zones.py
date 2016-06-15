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

def work_zones():
	auth = json.loads(open(sys.argv[1]).read())['services']['cityofbostondataportal']

	client = pymongo.MongoClient()
	repo = client.repo
	repo.authenticate('ebrakke_twaltze', 'ebrakke_twaltze')

	repo.dropPermanent('workZones')
	repo.createPermanent('workZones')

	offset = 0
	max_requests = 50000
	url = '{}resource/hx38-wur4.json?$limit={}&$offset={}&$$app_token={}'

	start_time = datetime.datetime.now()
	while True:
		this_request = url.format(auth['service'], max_requests, offset, auth['token'])
		print(this_request)
		response = urllib.request.urlopen(this_request).read().decode('utf-8')
		r = json.loads(response)
		if len(r) == 0:
			break
		repo['ebrakke_twaltze.workZones'].insert_many(r)
		offset += max_requests
		time.sleep(2)
	end_time = datetime.datetime.now()
	do_prov(start_time, end_time)

def do_prov(start_time=None, end_time=None):
	doc = prov.model.ProvDocument()
	doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ebrakke_twaltze/')
	doc.add_namespace('dat', 'http://datamechanics.io/data/ebrakke_twaltze/')
	doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
	doc.add_namespace('log', 'http://datamechanics.io/log#')
	doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

	script = doc.agent('alg:work_zones', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
	work_zones = doc.entity('bdp:hx38-wur4', {prov.model.PROV_LABEL:'Public Works Zones', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

	get_work_zones = doc.activity('log:a' + str(uuid.uuid4()), start_time, end_time, {prov.model.PROV_LABEL: 'Fetch all work zones'})
	doc.wasAssociatedWith(get_work_zones, script)
	doc.usage(get_work_zones, work_zones, start_time, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

	work = doc.entity('dat:workZones', {prov.model.PROV_LABEL:'Work Zones', prov.model.PROV_TYPE:'ont:DataSet'})
	doc.wasAttributedTo(work, script)
	doc.wasGeneratedBy(work, get_work_zones, end_time)
	doc.wasDerivedFrom(work, work_zones, get_work_zones, get_work_zones, get_work_zones)

	repo.record(doc.serialize())
	print(doc.get_provn())

if __name__ == '__main__':
	if len(sys.argv) < 2:
		print('Please provide a path to the authorization file')
		exit(1)
	work_zones()
