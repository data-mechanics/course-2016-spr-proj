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

def service_calls():
	auth = json.loads(open(sys.argv[1]).read())['services']['cityofbostondataportal']

	client = pymongo.MongoClient()
	repo = client.repo
	repo.authenticate('ebrakke_twaltze', 'ebrakke_twaltze')


	repo.dropPermanent('serviceCalls')
	repo.createPermanent('serviceCalls')

	offset = 0
	max_requests = 50000
	url = '{}resource/wc8w-nujj.json?$limit={}&$offset={}&$$app_token={}'
	start_time = datetime.datetime.now()
	while True:
		this_request = url.format(auth['service'], max_requests, offset, auth['token'])
		print(this_request)
		response = urllib.request.urlopen(this_request).read().decode('utf-8')
		r = json.loads(response)
		if len(r) == 0:
			break
		repo['ebrakke_twaltze.serviceCalls'].insert_many(r)
		offset += max_requests
		time.sleep(2)
	end_time = datetime.datetime.now()

	do_prov()

def do_prov(start_time=None, end_time=None):
	doc = prov.model.ProvDocument()
	doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ebrakke_twaltze/')
	doc.add_namespace('dat', 'http://datamechanics.io/data/ebrakke_twaltze/')
	doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
	doc.add_namespace('log', 'http://datamechanics.io/log#')
	doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

	script = doc.agent('alg:311_requests', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
	service_requests = doc.entity('bdp:wc8w-nujj', {prov.model.PROV_LABEL:'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

	get_service_requests = doc.activity('log:a' + str(uuid.uuid4()), start_time, end_time, {prov.model.PROV_LABEL: 'Fetch all service requests'})
	doc.wasAssociatedWith(get_service_requests, script)
	doc.usage(get_service_requests, service_requests, start_time, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

	service_calls = doc.entity('dat:serviceCalls', {prov.model.PROV_LABEL:'Service Requests', prov.model.PROV_TYPE:'ont:DataSet'})
	doc.wasAttributedTo(service_calls, script)
	doc.wasGeneratedBy(service_calls, get_service_requests, end_time)
	doc.wasDerivedFrom(service_calls, service_requests, get_service_requests, get_service_requests, get_service_requests)

	repo.record(doc.serialize())
	print(doc.get_provn())

if __name__ == '__main__':
	if len(sys.argv) < 2:
		print('Please provide a path to the authorization file')
		exit(1)
	service_calls()
