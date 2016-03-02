import pymongo
import prov.model
import json
import urllib.request
import uuid
import sys
import subprocess
from datetime import datetime


exec(open('../pymongo_dm.py').read())

client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('ebrakke_twaltze', 'ebrakke_twaltze')


def fetch_311_calls():
    start_time = datetime.now()
    run_python_script('311_requests.py')
    end_time = datetime.now()

    doc = prov_init(('bdp', 'https://data.cityofboston.gov/resource/'))

    script = log_script(doc, name='311_requests', ext='py')
    service_requests = create_entity(doc, 'bdp:wc8w-nujj', '311, Service Calls', 'ont:DataResource', 'json')
    
    get_service_requests = create_activity(doc, start_time, end_time)
    doc.wasAssociatedWith(get_service_requests, script)
    doc.usage(get_service_requests, service_requests, start_time, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

    service_calls = create_entity(doc, 'dat:serviceCalls', '311 Service Calls', 'ont:DataSet')
    doc.wasAttributedTo(service_calls, script)
    doc.wasGeneratedBy(service_calls, get_service_requests, end_time)
    doc.wasDerivedFrom(service_calls, service_requests, get_service_requests, get_service_requests, get_service_requests)

    prov_finish(doc)


def fetch_crime_reports():
    start_time = datetime.now()
    run_python_script('crime_reports.py')
    end_time = datetime.now()
    
    doc = prov_init(('bdp', 'https://data.cityofboston.gov/resource/'))
    
    script = log_script(doc, name='crime_reports', ext='py')
    crime_reports = create_entity(doc, 'bdp:ufcx-3fdn', 'Crime Reports', 'ont:DataResource', 'json')
    
    get_crime = create_activity(doc, start_time, end_time)
    doc.wasAssociatedWith(get_crime, script)
    doc.usage(get_crime, crime_reports, start_time, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

    crime = create_entity(doc, 'dat:crimeReports', 'Crime Reports', 'ont:DataSet')
    doc.wasAttributedTo(crime, script)
    doc.wasGeneratedBy(crime, get_crime, end_time)
    doc.wasDerivedFrom(crime, crime_reports, get_crime, get_crime, get_crime)

    prov_finish(doc)

def fetch_work_zones():
    start_time = datetime.now()
    run_python_script('work_zones.py')
    end_time = datetime.now()

    doc = prov_init(('bdp', 'https://data.cityofboston.gov/resource/'))
    
    script = log_script(doc, name='work_zones', ext='py')
    work_zones = create_entity(doc, 'bdp:hx38-wur4', 'Public Works Active Work Zones', 'ont:DataResource', 'json')

    get_zones = create_activity(doc, start_time, end_time)
    doc.wasAssociatedWith(get_zones, script)
    doc.usage(get_zones, work_zones, start_time, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

    zones = create_entity(doc, 'dat:workZones', 'Work Zones', 'ont:DataSet')
    doc.wasAttributedTo(zones, script)
    doc.wasGeneratedBy(zones, get_zones, end_time)
    doc.wasDerivedFrom(zones, work_zones, get_zones, get_zones, get_zones)

    prov_finish(doc)

def filter_potholes():
    start_time = datetime.now()
    run_mongo_script('potholeFilter.js')
    end_time = datetime.now()
	
    doc = prov_init()

    script = log_script(doc, name='potholeFilter', ext='js')
    service_calls = create_entity(doc, 'dat:serviceCalls', '311 Service Calls', 'ont:DataSet')

    filter_potholes = create_activity(doc, start_time, end_time)
    doc.wasAssociatedWith(filter_potholes, script)
    doc.usage(filter_potholes, service_calls, start_time, None, {prov.model.PROV_TYPE:'ont:Computation'})
    
    potholes = create_entity(doc, 'dat:potholes', 'Potholes', 'ont:DataSet')
    doc.wasAttributedTo(potholes, script)
    doc.wasGeneratedBy(potholes, filter_potholes, end_time)
    doc.wasDerivedFrom(potholes, service_calls, filter_potholes, filter_potholes, filter_potholes)

    prov_finish(doc)

def filter_construction():
    start_time = datetime.now()
    run_mongo_script('constructionFilter.js')
    end_time = datetime.now()

    doc = prov_init()

    script = log_script(doc, name='constructionFilter', ext='js')
    work_zones = create_entity(doc, 'dat:workZones', 'Public Works Zones', 'ont:DataSet')
    
    filter_construction = create_activity(doc, start_time, end_time)
    doc.wasAssociatedWith(filter_construction, script)
    doc.usage(filter_construction, work_zones, start_time, None, {prov.model.PROV_TYPE:'ont:DataSet'})
    
    construction = create_entity(doc, 'dat:construction', 'Construction', 'ont;DataSet')
    doc.wasAttributedTo(construction, script)
    doc.wasGeneratedBy(construction, filter_construction, end_time)
    doc.wasDerivedFrom(construction, work_zones, filter_construction, filter_construction, filter_construction)
    
    prov_finish(doc)

def filter_accidents():
    start_time = datetime.now()
    run_mongo_script('accidentFilter.js')
    end_time = datetime.now()
    
    doc = prov_init()
    
    script = log_script(doc, name='accidentFilter', ext='js')
    crime_reports = create_entity(doc, 'dat:crimeReports', 'Crime Reports', 'ont:DataSet')

    filter_crime = create_activity(doc, start_time, end_time)
    doc.wasAssociatedWith(filter_crime, script)
    doc.usage(filter_crime, crime_reports, start_time, None, {prov.model.PROV_TYPE:'ont:DataSet'})
    
    accidents = create_entity(doc, 'dat:accidents', 'Motor Accidents', 'ont:DataSet')
    doc.wasAttributedTo(accidents, script)
    doc.wasGeneratedBy(accidents, filter_crime, end_time)
    doc.wasDerivedFrom(accidents, crime_reports, filter_crime, filter_crime, filter_crime)
    
    prov_finish(doc)

def calculate_danger_levels():
    start_time = datetime.now()
    success = run_python_script('calculate_danger_levels.py')
    if not success:
        return
    end_time = datetime.now()

    doc = prov_init()

    script = log_script(doc, name='calculate_danger_levels', ext='py')
    potholes = create_entity(doc, 'dat:potholes', 'Potholes', 'ont:DataSet')
    work_zones = create_entity(doc, 'dat:construction', 'Construction', 'ont:DataSet')
    accidents = create_entity(doc, 'dat:accidents', 'Motor Accidents', 'ont:DataSet')

    calc_danger = create_activity(doc, start_time, end_time)
    doc.wasAssociatedWith(calc_danger, script)
    doc.usage(calc_danger, potholes, start_time, None, {prov.model.PROV_TYPE:'ont:DataSet'})
    doc.usage(calc_danger, work_zones, start_time, None, {prov.model.PROV_TYPE:'ont:DataSet'})
    doc.usage(calc_danger, accidents, start_time, None, {prov.model.PROV_TYPE:'ont:DataSet'})
            
    danger_levels = create_entity(doc, 'dat:dangerLevels', 'Danger Levels', 'ont:DataSet')
    doc.wasAttributedTo(danger_levels, script)
    doc.wasGeneratedBy(danger_levels, calc_danger, end_time)
    doc.wasDerivedFrom(danger_levels, potholes, calc_danger, calc_danger, calc_danger)
    doc.wasDerivedFrom(danger_levels, work_zones, calc_danger, calc_danger, calc_danger)
    doc.wasDerivedFrom(danger_levels, accidents, calc_danger, calc_danger, calc_danger)
    
    prov_finish(doc)

def prov_init(*args):
    ## Prov setup
    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/alice_bob/') # The scripts in / format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/alice_bob/') # The data sets in / format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    #Add any other prov namespaces needed
    for (label, url) in args:
        doc.add_namespace(label, url) 

    return doc

def prov_finish(doc):
    repo.record(doc.serialize())
    print(doc.get_provn())

def create_entity(doc, entity_label, prov_label, ty, extension=None):
    if extension:
        return doc.entity(entity_label, {prov.model.PROV_LABEL:prov_label, prov.model.PROV_TYPE:ty, 'ont:Extension':extension}) 
    return doc.entity(entity_label, {prov.model.PROV_LABEL:prov_label, prov.model.PROV_TYPE:ty})

def create_activity(doc, start, end):
    return doc.activity('log:a' + str(uuid.uuid4()), start, end)

def log_script(doc, **kwargs):
	doc.agent('alg:{}'.format(kwargs['name']), {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': kwargs['ext']})

def run_mongo_script(script_name):
    subprocess.call('mongo repo -u ebrakke_twaltze -p ebrakke_twaltze --authenticationDatabase "repo" {}'.format(script_name), shell=True)

def run_python_script(script_name):
    try:
        subprocess.check_output('python3 {}'.format(script_name), shell=True)
        return True
    except subprocess.CalledProcessError as e:
        print(e.output)
        return False

arg_mapping = {
        'filter_potholes': filter_potholes,
        'filter_accidents': filter_accidents,
        'filter_construction': filter_construction,
        'fetch_311_calls': fetch_311_calls,
        'fetch_crime_reports': fetch_crime_reports,
        'fetch_work_zones': fetch_work_zones,
        'calculate_danger_levels': calculate_danger_levels
    }

if __name__ == '__main__':
    for f in sys.argv[1:]:
        arg_mapping.get(f)()
    repo.logout()
