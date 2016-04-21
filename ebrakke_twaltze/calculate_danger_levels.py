import pymongo
import prov.model
import datetime
import uuid

from DangerZones import Pothole
from DangerZones import Accident
from DangerZones import Construction

exec(open('../pymongo_dm.py').read())

client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('ebrakke_twaltze', 'ebrakke_twaltze')

def calculate_danger_levels():
    start_time = datetime.datetime.now()


    # Collection to store the danger levels to
    collection = 'dangerLevels'
    repo.dropPermanent(collection)
    repo.createPermanent(collection)

    datasets = ['potholes', 'construction', 'accidents']
    data = []
    for dataset in datasets:
        dataset = repo['ebrakke_twaltze.' + dataset].find()

        for d in dataset:
            dangerZoneType = d.get('type')
            print(d)
            # Determine the type of zone
            if dangerZoneType == 'pothole':
                dangerZone = Pothole(d.get('latitude'), d.get('longitude'), d.get('report_dt'), d.get('status'))
            elif dangerZoneType == 'construction':
                dangerZone = Construction(d.get('latitude'), d.get('longitude'), d.get('expected_close_date'), d.get('num_sidewalk_plates'), d.get('num_road_plates'))
            elif dangerZoneType == 'accident':
                dangerZone = Accident(d.get('latitude'), d.get('longitude'), d.get('report_dt'))

            # Get the danger level for this data point
            dangerLevel = dangerZone.calculateDangerLevel()

            # Insert into db
            data.append({
                'lat': float(d.get('latitude')),
                'lng': float(d.get('longitude')),
                'dangerLevel': dangerLevel,
                'type': d.get('type')
            })

    repo['ebrakke_twaltze.' + collection].insert_many(data)
    end_time = datetime.datetime.now()

    do_prov(start_time, end_time)

def do_prov(start_time, end_time):
    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ebrakke_twaltze/')
    doc.add_namespace('dat', 'http://datamechanics.io/data/ebrakke_twaltze/')
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
    doc.add_namespace('log', 'http://datamechanics.io/log#')

    script = doc.agent('alg:calculate_danger_levels', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
    construction = doc.entity('dat:construction', {prov.model.PROV_LABEL:'Construction', prov.model.PROV_TYPE:'ont:DataSet'})
    potholes = doc.entity('dat:potholes', {prov.model.PROV_LABEL:'Potholes', prov.model.PROV_TYPE:'ont:DataSet'})
    accidents = doc.entity('dat:accidents', {prov.model.PROV_LABEL:'Accidents', prov.model.PROV_TYPE:'ont:DataSet'})

    calc_danger = doc.activity('log:a' + str(uuid.uuid4()), start_time, end_time, {prov.model.PROV_LABEL: 'Map each point to a danger level'})
    doc.wasAssociatedWith(calc_danger, script)

    doc.usage(calc_danger, potholes, start_time, None, {prov.model.PROV_TYPE:'ont:Computation'})
    doc.usage(calc_danger, construction, start_time, None, {prov.model.PROV_TYPE:'ont:Computation'})
    doc.usage(calc_danger, accidents, start_time, None, {prov.model.PROV_TYPE:'ont:Computation'})

    danger_levels = doc.entity('dat:dangerLevels', {prov.model.PROV_LABEL: 'Danger Levels', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(danger_levels, script)
    doc.wasGeneratedBy(danger_levels, calc_danger, end_time)
    doc.wasDerivedFrom(danger_levels, potholes, calc_danger, calc_danger, calc_danger)
    doc.wasDerivedFrom(danger_levels, construction, calc_danger, calc_danger, calc_danger)
    doc.wasDerivedFrom(danger_levels, accidents, calc_danger, calc_danger, calc_danger)

    repo.record(doc.serialize())
    print(doc.get_provn())

if __name__ == '__main__':
    calculate_danger_levels()
