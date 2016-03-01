import pymongo
import prov.model
import json
import urllib.request
import uuid
import sys
from datetime import datetime
from subprocess import call


exec(open('../pymongo_dm.py').read())

client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('ebrakke_twaltze', 'ebrakke_twaltze')

## Prov setup
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/alice_bob/') # The scripts in / format.
doc.add_namespace('dat', 'http://datamechanics.io/data/alice_bob/') # The data sets in / format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'http://data.cityofboston.gov/resource') # Boston Data Portal
def fetch_311_calls():
    start_time = datetime.now()
    run_python_script('311_requests.py')
    end_time = datetime.now()

    script = log_script(name='311_requests', ext='py')
    service_requests = create_entity('bdp:wc8w-nujj', '311, Service Calls', 'ont:DataResource', 'json')
    
    get_service_requests = create_activity(start_time, end_time)
    doc.wasAssociatedWith(get_service_requests, script)
    doc.usage(get_service_requests, service_requests, start_time, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

    service_calls = create_entity('dat:serviceCalls', '311 Service Calls', 'ont:DataSet')
    doc.wasAttributedTo(service_calls, script)
    doc.wasGeneratedBy(service_calls, get_service_requests, end_time)
    doc.wasDerivedFrom(service_calls, service_requests, get_service_requests, get_service_requests, get_service_requests)
    repo.record(doc.serialize())

    print(doc.get_provn())

def fetch_crime_reports():
    start_time = datetime.now()
    run_python_script('crime_reports.py')
    end_time = datetime.now()

    script = log_script(name='crime_reports', ext='py')
    crime_reports = create_entity('bdp:ufcx-3fdn', 'Crime Reports', 'ont:DataResource', 'json')
    
    get_crime = create_activity(start_time, end_time)
    doc.wasAssociatedWith(get_crime, script)
    doc.usage(get_crime, crime_reports, start_time, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

    crime = create_entity('dat:crimeReports', 'Crime Reports', 'ont:DataSet')
    doc.wasAttributedTo(crime, script)
    doc.wasGeneratedBy(crime, get_crime, end_time)
    doc.wasDerivedFrom(crime, crime_reports, get_crime, get_crime, get_crime)
    repo.record(doc.serialize())

    print(doc.get_provn())

def filter_potholes():
    start_time = datetime.now()
    run_mongo_script('potholeFilter.js')
    end_time = datetime.now()
	
    #Prov data
    script = log_script(name='potholeFilter', ext='js')
    service_calls = create_entity('dat:serviceCalls', '311 Service Calls', 'ont:DataSet')

    filter_potholes = create_activity(start_time, end_time)
    doc.wasAssociatedWith(filter_potholes, script)
    doc.usage(filter_potholes, service_calls, start_time, None, {prov.model.PROV_TYPE:'ont:Computation'})
    
    potholes = create_entity('date:potholes', 'Potholes', 'ont:DataSet')
    doc.wasAttributedTo(potholes, script)
    doc.wasGeneratedBy(potholes, filter_potholes, end_time)
    doc.wasDerivedFrom(potholes, service_calls, filter_potholes, filter_potholes, filter_potholes)
    repo.record(doc.serialize())

    print(doc.get_provn())

def filter_construction():
    start_time = datetime.now()
    run_mongo_script('constructionFilter.js')
    end_time = datetime.now()

    script = log_script(name='constructionFilter', ext='js')
    work_zones = create_entity('dat:workZones', 'Public Works Zones', 'ont:DataSet')
    
    filter_construction = create_activity(start_time, end_time)
    doc.wasAssociatedWith(filter_construction, script)
    doc.usage(filter_construction, work_zones, start_time, None, {prov.model.PROV_TYPE:'ont:DataSet'})
    
    construction = create_entity('dat:construction', 'Construction', 'ont;DataSet')
    doc.wasAttributedTo(construction, script)
    doc.wasGeneratedBy(construction, filter_construction, end_time)
    doc.wasDerivedFrom(construction, work_zones, filter_construction, filter_construction, filter_construction)
    repo.record(doc.serialize())

    print(doc.get_provn())


def filter_accidents():
    start_time = datetime.now()
    run_mongo_script('accidentFilter.js')
    end_time = datetime.now()

    script = log_script(name='accidentFilter', ext='js')
    crime_reports = create_entity('dat:crimeReports', 'Crime Reports', 'ont:DataSet')

    filter_crime = create_activity(start_time, end_time)
    doc.wasAssociatedWith(filter_crime, script)
    accidents = create_entity('dat:accidents', 'Motor Accidents', 'ont:DataSet')
    doc.wasAttributedTo(accidents, script)
    doc.wasGeneratedBy(accidents, filter_crime, end_time)
    doc.wasDerivedFrom(accidents, crime_reports, filter_crime, filter_crime, filter_crime)
    repo.record(doc.serialize())

    print(doc.get_provn())

def create_entity(entity_label, prov_label, ty, extension=None):
    if extension:
        return doc.entity(entity_label, {prov.model.PROV_LABEL:prov_label, prov.model.PROV_TYPE:ty, 'ont:Extension':extension}) 
    return doc.entity(entity_label, {prov.model.PROV_LABEL:prov_label, prov.model.PROV_TYPE:ty})

def create_activity(start, end):
    return doc.activity('log:a' + str(uuid.uuid4()), start, end)

def log_script(**kwargs):
	doc.agent('alg:{}'.format(kwargs['name']), {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': kwargs['ext']})

def run_mongo_script(script_name):
    call('mongo repo -u ebrakke_twaltze -p ebrakke_twaltze --authenticationDatabase "repo" {}'.format(script_name), shell=True)

def run_python_script(script_name):
    call('python3 {}'.format(script_name), shell=True)

arg_mapping = {
        'filter_potholes': filter_potholes,
        'filter_accidents': filter_accidents,
        'filter_construction': filter_construction,
        'fetch_311_calls': fetch_311_calls,
        'fetch_crime_reports': fetch_crime_reports
    }

if __name__ == '__main__':
    for f in sys.argv[1:]:
        arg_mapping.get(f)()
    repo.logout()
