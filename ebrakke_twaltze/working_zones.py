import prov.model
import pymongo
import datetime
import urllib.request
import time
import uuid

exec(open('../pymongo_dm.py').read())

config = json.loads(open('auth.json').read())

client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('ebrakke_twaltze', 'ebrakke_twaltze')


start_time = datetime.datetime.now()
repo.dropPermanent('workZones')
repo.createPermanent('workZones')

offset = 0
max_requests = 50000
url = 'https://data.cityofboston.gov/resource/hx38-wur4.json?$limit={}&$offset={}&$$app_token={}'

while True:
	print(url.format(max_requests, offset, config['socrataToken']))
	response = urllib.request.urlopen(url.format(max_requests, offset, config['socrataToken'])).read().decode('utf-8')
	r = json.loads(response)
	if len(r) == 0:
		break
	repo['ebrakke_twaltze.workZones'].insert_many(r)
	offset += max_requests
	time.sleep(2)
end_time = datetime.datetime.now()

### PROV DATA ###
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ebrakke_twaltze/')
doc.add_namespace('dat', 'http://datamechanics.io/data/ebrakke_twaltze/')
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:working_zones', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
resource = doc.entity('bdp:hx38-wur4', {'prov:label':'Public Works Active Work Zones', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
this_run = doc.activity('log:a'+str(uuid.uuid4()), start_time, end_time, {prov.model.PROV_TYPE:'ont:Retrieval'})
doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, resource, start_time)

service_requests = doc.entity('dat:work_zones', {prov.model.PROV_LABEL:'Public Works Zone', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(service_requests, this_script)
doc.wasGeneratedBy(service_requests, this_run, end_time)
doc.wasDerivedFrom(service_requests, resource, this_run, this_run, this_run)

repo.record(doc.serialize())
print(doc.get_provn())
repo.logout()
