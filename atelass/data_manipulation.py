'''
This script creates three new collections: repo['atelass.crimes_and_stations'],
repo['atelass.stations_and_streetlights'], and repo['atelass.crimes_and_streetlights'].

repo['atelass.crimes_and_stations'] goes through repo['atelass.stations'] and makes sure that
each station nearest a crime is within a dist_to_station distance from that crime. This collection
may be the same as repo['atelass.stations'], depending on the value of dist_to_station.

repo['atelass.stations_and_streetlights'] gets all the streetlights within a
(dist_to_station + dist_to_streetlight) distance from a station.

repo['atelass.crimes_and_streetlights'] gets all the streetlights within a dist_to_streetlight distance from a crime.
'''

import pymongo
import json
import time
import prov.model
import datetime
import uuid
from haversine import haversine

# Until a library is created, we just use the script directly.
# Path of pymongo_dm.py may need to be changed to ../pymongo_dm.py.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('atelass', 'atelass')

# Set values for dist_to_station and dist_to_streetlight in miles.
dist_to_station = 500.0 / 5280.0    # convert 500 ft to miles
dist_to_streetlight = 50.0 / 5280.0    # convert 50 ft to miles

# Go through stations nearest a crime and only take those that are within a dist_to_station distance
# from the crime.
print(time.strftime('%I:%M:%S%p') + ': Getting stations within a ' + str(dist_to_station) + ' mile distance from a crime.')
crimes_and_stations = []
crimes_and_stations_start_time = datetime.datetime.now()
for doc in repo['atelass.stations'].find():
    if float(doc['station_info']['distance']) <= dist_to_station:
        crimes_and_stations.append(doc)
crimes_and_stations_end_time = datetime.datetime.now()

repo.dropPermanent("crimes_and_stations")
repo.createPermanent("crimes_and_stations")
repo['atelass.crimes_and_stations'].insert_many(crimes_and_stations)

print(time.strftime('%I:%M:%S%p') + ': Completed.')

# Get locations of all streetlights as (lat, lon) pairs.
print(time.strftime('%I:%M:%S%p') + ': Getting locations of all streetlights.')
stations_and_streetlights_start_time = datetime.datetime.now()
streetlight_locations = []
for doc in repo['atelass.streetlights'].find():
    lat = float(doc['properties']['lat'])
    lon = float(doc['properties']['long'])
    loc = (lat, lon)
    streetlight_locations.append(loc)
print(time.strftime('%I:%M:%S%p') + ': Completed.')

# Get all streetlights within a (dist_to_station + dist_to_streetlight) mile radius of station.
print(time.strftime('%I:%M:%S%p') + ': Getting streetlights within a ' + str(dist_to_station + dist_to_streetlight) + ' mile distance from a station.')
stations = []
stations_and_streetlights = []
for doc in repo['atelass.crimes_and_stations'].find():
    station_name = doc['station_info']['stop_name']
    if station_name not in stations:    # To make sure we're not redoing a station we've already done
        stations.append(station_name)
        station_lat = float(doc['station_info']['stop_lat'])
        station_lon = float(doc['station_info']['stop_lon'])
        station_location = (station_lat, station_lon)
        streetlights_around_station = []
        for streetlight_location in streetlight_locations:
            if haversine(station_location, streetlight_location, miles=True) <= (dist_to_station + dist_to_streetlight):
                streetlights_around_station.append(streetlight_location)
        stations_and_streetlights.append({'station': station_name, 'streetlight_locations': streetlights_around_station})
stations_and_streetlights_end_time = datetime.datetime.now()

repo.dropPermanent("stations_and_streetlights")
repo.createPermanent("stations_and_streetlights")
repo['atelass.stations_and_streetlights'].insert_many(stations_and_streetlights)

print(time.strftime('%I:%M:%S%p') + ': Completed.')

# Get the ID of the crime (according to documentation, compnos is the internal BPD report number).
print(time.strftime('%I:%M:%S%p') + ': Getting streetlights within a ' + str(dist_to_streetlight) + ' mile distance from a crime.')
crime_compnoses = []
for doc in repo['atelass.crimes_and_stations'].find():
    crime_compnos = doc['crime_compnos']
    crime_compnoses.append(crime_compnos)

# Go through crimes with a compnos in crime_compnoses and get all streetlights within a dist_to_streetlight
# distance from the crime.
crimes_and_streetlights = []
crimes_and_streetlights_start_time = datetime.datetime.now()
for crime_compnos in crime_compnoses:
    crime_lat = float(repo['atelass.crimes'].find({'compnos': crime_compnos})[0]['location']['latitude'])
    crime_lon = float(repo['atelass.crimes'].find({'compnos': crime_compnos})[0]['location']['longitude'])
    crime_location = (crime_lat, crime_lon)
    station_name = repo['atelass.crimes_and_stations'].find({'crime_compnos': crime_compnos})[0]['station_info']['stop_name']
    streetlight_locations = []
    for streetlight_location in repo['atelass.stations_and_streetlights'].find({'station': station_name})[0]['streetlight_locations']:
        if haversine(crime_location, streetlight_location, miles=True) <= dist_to_streetlight:
            streetlight_locations.append(streetlight_location)
    crimes_and_streetlights.append({'crime_compnos': crime_compnos, 'streetlight_locations': streetlight_locations})
crimes_and_streetlights_end_time = datetime.datetime.now()

repo.dropPermanent("crimes_and_streetlights")
repo.createPermanent("crimes_and_streetlights")
repo['atelass.crimes_and_streetlights'].insert_many(crimes_and_streetlights)

print(time.strftime('%I:%M:%S%p') + ': Completed.')


doc = prov.model.ProvDocument.deserialize('plan.json')

# Provenance information
doc2 = prov.model.ProvDocument()
doc2.add_namespace('alg', 'http://datamechanics.io/algorithm/atelass/')
doc2.add_namespace('dat', 'http://datamechanics.io/data/atelass/')
doc2.add_namespace('ont', 'http://datamechanics.io/ontology#')
doc2.add_namespace('log', 'http://datamechanics.io/log#')

this_script = doc2.agent('alg:data_manipulation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

# Crimes_and_stations provenance information
get_crimes_and_stations = doc2.activity('log:a'+str(uuid.uuid4()), crimes_and_stations_start_time, crimes_and_stations_end_time)
doc2.wasAssociatedWith(get_crimes_and_stations, this_script)
doc2.used(get_crimes_and_stations, 'dat:stations', crimes_and_stations_start_time)

crimes_and_stations = doc2.entity('dat:crimes_and_stations', {prov.model.PROV_LABEL:'Crimes and Stations', prov.model.PROV_TYPE:'ont:DataSet'})
doc2.wasAttributedTo(crimes_and_stations, this_script)
doc2.wasGeneratedBy(crimes_and_stations, get_crimes_and_stations, crimes_and_stations_end_time)
doc2.wasDerivedFrom(crimes_and_stations, 'dat:stations', get_crimes_and_stations)

# Stations_and_streetlights provenance information
get_stations_and_streetlights = doc2.activity('log:a'+str(uuid.uuid4()), stations_and_streetlights_start_time, stations_and_streetlights_end_time)
doc2.wasAssociatedWith(get_stations_and_streetlights, this_script)
doc2.used(get_stations_and_streetlights, 'dat:streetlights', stations_and_streetlights_start_time)
doc2.used(get_stations_and_streetlights, crimes_and_stations, stations_and_streetlights_start_time)

stations_and_streetlights = doc2.entity('dat:stations_and_streetlights', {prov.model.PROV_LABEL:'Stations and Streetlights', prov.model.PROV_TYPE:'ont:DataSet'})
doc2.wasAttributedTo(stations_and_streetlights, this_script)
doc2.wasGeneratedBy(stations_and_streetlights, get_stations_and_streetlights, stations_and_streetlights_end_time)
doc2.wasDerivedFrom(stations_and_streetlights, 'dat:streetlights', get_stations_and_streetlights)
doc2.wasDerivedFrom(stations_and_streetlights, crimes_and_stations, get_stations_and_streetlights)

# Crimes_and_streetlights provenance information
get_crimes_and_streetlights = doc2.activity('log:a'+str(uuid.uuid4()), crimes_and_streetlights_start_time, crimes_and_stations_end_time)
doc2.wasAssociatedWith(get_crimes_and_streetlights, this_script)
doc2.used(get_crimes_and_streetlights, 'dat:crimes', stations_and_streetlights_start_time)
doc2.used(get_crimes_and_streetlights, crimes_and_stations, stations_and_streetlights_start_time)
doc2.used(get_crimes_and_streetlights, stations_and_streetlights, stations_and_streetlights_start_time)

crimes_and_streetlights = doc2.entity('dat:crimes_and_streetlights', {prov.model.PROV_LABEL:'Crimes and Streetlights', prov.model.PROV_TYPE:'ont:DataSet'})
doc2.wasAttributedTo(crimes_and_streetlights, this_script)
doc2.wasGeneratedBy(crimes_and_streetlights, get_crimes_and_streetlights, crimes_and_streetlights_end_time)
# get_crimes_and_streetlights used the crimes dataset, but crimes_and_streetlights wasn't derived from it - only from the following two datasets
doc2.wasDerivedFrom(crimes_and_streetlights, crimes_and_stations, get_crimes_and_streetlights)
doc2.wasDerivedFrom(crimes_and_streetlights, stations_and_streetlights, get_crimes_and_streetlights)

doc.update(doc2)

repo.record(doc.serialize())
open('plan.json', 'w').write(json.dumps(json.loads(doc.serialize()), indent=4))
#print(doc.get_provn())
repo.logout()
