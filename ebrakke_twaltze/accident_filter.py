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


def accident_filter():
    start_time = datetime.datetime.now()
    try:
        subprocess.check_output('mongo repo -u ebrakke_twaltze -p ebrakke_twaltze --authenticationDatabase "repo" accidentFilter.js', shell=True)
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

	script = doc.agent('alg:accidentFilter', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'js'})
	crime = doc.entity('dat:crimeReports', {prov.model.PROV_LABEL:'Crime Reports', prov.model.PROV_TYPE:'ont:DataSet'})

	filter_accidents = doc.activity('log:a' + str(uuid.uuid4()), start_time, end_time, {prov.model.PROV_LABEL: 'Filter service calls for potholes'})
	doc.wasAssociatedWith(filter_accidents, script)
	doc.usage(filter_accidents, crime, start_time, None, {prov.model.PROV_TYPE:'ont:Computation'})

	accidents = doc.entity('dat:accidents', {prov.model.PROV_LABEL:'Accidents', prov.model.PROV_TYPE:'ont:DataSet'})
	doc.wasAttributedTo(accidents, script)
	doc.wasGeneratedBy(accidents, filter_accidents, end_time)
	doc.wasDerivedFrom(accidents, crime, filter_accidents, filter_accidents, filter_accidents)

	repo.record(doc.serialize())
	print(doc.get_provn())

if __name__ == '__main__':
    accident_filter()
