from DangerZones import Pothole
from DangerZones import Accident
from DangerZones import Construction

import pymongo
import prov.model
import sys

exec(open('../pymongo_dm.py').read())

client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('ebrakke_twaltze', 'ebrakke_twaltze')

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
            'lat': d.get('latitude'),
            'lng': d.get('longitude'),
            'dangerLevel': dangerLevel,
            'type': d.get('type')
        })

repo['ebrakke_twaltze.' + collection].insert_many(data)
