import prov.model
import dml
import datetime
import time
import uuid
import sys
import subprocess

exec(open('../pymongo_dm.py').read())

client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('ebrakke_twaltze', 'ebrakke_twaltze')

def filter_potholes():
    start_time = datetime.datetime.now()
    try:
        subprocess.check_output('mongo repo -u ebrakke_twaltze -p ebrakke_twaltze --authenticationDatabase "repo" potholeFilter.js', shell=True)
        end_time = datetime.datetime.now()
        do_prov(start_time, end_time)
    except subprocess.CalledProcessError as e:
        print(e.output)

def do_prov(start_time=None, end_time=None):
	doc = prov.model.ProvDocument()
	doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ebrakke_twaltze/')
	doc.add_namespace('dat', 'http://datamechanics.io/data/ebrakke_twaltze/')
	doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
	doc.add_namespace('log', 'http://datamechanics.io/log#')

	script = doc.agent('alg:potholeFilter', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'js'})
	service_calls = doc.entity('dat:serviceCalls', {prov.model.PROV_LABEL:'311 Service Requests', prov.model.PROV_TYPE:'ont:DataSet'})

	filter_potholes = doc.activity('log:a' + str(uuid.uuid4()), start_time, end_time, {prov.model.PROV_LABEL: 'Filter service calls for potholes'})
	doc.wasAssociatedWith(filter_potholes, script)
	doc.usage(filter_potholes, service_calls, start_time, None, {prov.model.PROV_TYPE:'ont:Computation'})

	potholes = doc.entity('dat:potholes', {prov.model.PROV_LABEL:'Potholes', prov.model.PROV_TYPE:'ont:DataSet'})
	doc.wasAttributedTo(potholes, script)
	doc.wasGeneratedBy(potholes, filter_potholes, end_time)
	doc.wasDerivedFrom(potholes, service_calls, filter_potholes, filter_potholes, filter_potholes)

	repo.record(doc.serialize())
	print(doc.get_provn())

if __name__ == '__main__':
    filter_potholes()
