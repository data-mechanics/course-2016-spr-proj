import prov.model
import dml
import datetime
import time
import uuid
import sys
import subprocess

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('ebrakke_twaltze', 'ebrakke_twaltze')


def filter_construction():
    start_time = datetime.datetime.now()
    try:
        subprocess.check_output('mongo repo -u ebrakke_twaltze -p ebrakke_twaltze --authenticationDatabase "repo" constructionFilter.js', shell=True)
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

	script = doc.agent('alg:constructionFilter', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'js'})
	work_zones = doc.entity('dat:workZones', {prov.model.PROV_LABEL:'Public Works Zones', prov.model.PROV_TYPE:'ont:DataSet'})

	filter_construction = doc.activity('log:a' + str(uuid.uuid4()), start_time, end_time, {prov.model.PROV_LABEL: 'Filter work zones for construction'})
	doc.wasAssociatedWith(filter_construction, script)
	doc.usage(filter_construction, work_zones, start_time, None, {prov.model.PROV_TYPE:'ont:Computation'})

	construction = doc.entity('dat:construction', {prov.model.PROV_LABEL:'Construction', prov.model.PROV_TYPE:'ont:DataSet'})
	doc.wasAttributedTo(construction, script)
	doc.wasGeneratedBy(construction, filter_construction, end_time)
	doc.wasDerivedFrom(construction, work_zones, filter_construction, filter_construction, filter_construction)

	repo.record(doc.serialize())
	print(doc.get_provn())

if __name__ == '__main__':
    filter_construction()
