'''
This file scores each station. Scoring is done by getting the total crimes for each station for each hour between 9PM to
1AM (excluding 1AM) and calculating a weighted sum. Earlier hours are weighted more.
These times are chosen because most stations close around 1AM or a little before.
A higher score means the station is less safe, at least according to this metric and the results of this project.
'''

import pymongo
import time
import json
import prov.model
import datetime
import uuid
import math

# Until a library is created, we just use the script directly.
# Path of pymongo_dm.py may need to be changed to ../pymongo_dm.py.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('atelass', 'atelass')

stations_and_scores_start_time = datetime.datetime.now()
# Extract crime compnoses (unique identifier for a crime).
print(time.strftime('%I:%M:%S%p') + ': Extracting crime compnoses.')
crime_compnoses = []
for doc in repo['atelass.crimes_and_streetlights'].find():
    crime_compnoses.append(doc['crime_compnos'])
print(time.strftime('%I:%M:%S%p') + ': Completed.')

# For each station, get the hours of crimes that happened between 9PM and 1AM (not including anything at or after 1).
print(time.strftime('%I:%M:%S%p') + ': Getting crimes that happened between 9PM and 1AM.')
stations_and_crimes_with_times = {}
for crime_compnos in crime_compnoses:
    crime_hour = repo['atelass.crimes'].find({'compnos': crime_compnos})[0]['fromdate'].split('T')[1][0:2]
    if crime_hour in ['21', '22', '23', '00']:
        station = repo['atelass.crimes_and_stations'].find({'crime_compnos': crime_compnos})[0]['station_info']['stop_name']
        if station not in stations_and_crimes_with_times.keys():
            stations_and_crimes_with_times[station] = [crime_hour]
        else:
            stations_and_crimes_with_times[station].append(crime_hour)
print(time.strftime('%I:%M:%S%p') + ': Completed.')

# For each station, calculate the number of crimes for each hour.
print(time.strftime('%I:%M:%S%p') + ': Calculating number of crimes for each hour at each station.')
num_crimes_per_hour = {}
for station in stations_and_crimes_with_times.keys():
    num_crimes_per_hour[station] = {'21': 0, '22': 0, '23': 0, '00': 0}
    for hour in stations_and_crimes_with_times[station]:
        num_crimes_per_hour[station][hour] += 1
print(time.strftime('%I:%M:%S%p') + ': Completed.')

# Calculated weighted sums for each station. Crimes with hour=21 will be multiplied by 4, crimes with hour=22 will be multiplied
# by 3, crimes with hour=23 will be multiplied by 2, and crimes with hour=00 will be multiplied by 1.
scores = []
for station in num_crimes_per_hour.keys():
    score = 0
    for hour in num_crimes_per_hour[station]:
        if hour == '21':
            score += (4 * num_crimes_per_hour[station][hour])
        if hour == '22':
            score += (3 * num_crimes_per_hour[station][hour])
        if hour == '23':
            score += (2 * num_crimes_per_hour[station][hour])
        if hour == '00':
            score += (1 * num_crimes_per_hour[station][hour])
    scores.append([score, station])

# Store results as list of dictionaries to insert into repo.
station_scores = []
for station_and_score in scores:
    station = station_and_score[1]
    score = station_and_score[0]
    station_scores.append({'station': station, 'score': score})
stations_and_scores_end_time = datetime.datetime.now()

repo.dropPermanent("stations_and_scores")
repo.createPermanent("stations_and_scores")
repo['atelass.stations_and_scores'].insert_many(station_scores)

# Display results.
print()
print('Stations by score, from lowest to highest. A higher score implies a less safe station.')
scores.sort()
for station_and_score in scores:
    print('Score: ' + str(station_and_score[0]) + '\t\tStation: ' + station_and_score[1])

# Create scores.json file for visualization
def round_up(num):
    return int(math.ceil(num/100.0) * 100.0)

def round_down(num):
    return int(math.floor(num/100.0) * 100.0)

# Creating a dictionary for the data to be stored in the scoring visualization following
# the example at the following URL: http://bl.ocks.org/mbostock/7607535
scores_json = {"name": "scores", "children": []}
min_score = scores[0][0]
max_score = scores[-1][0]

bottom_min_range = round_down(min_score)
top_max_range = round_up(max_score)

ranges = list(range(bottom_min_range, top_max_range + 1, 100))
range_strings = []
for i in range(len(ranges) - 1):
    range_string = str(ranges[i]) + '-'
    range_string += str(ranges[i+1] - 1)
    range_strings.append(range_string)
    scores_json["children"].append({"name": range_string, "children": []})

for station_score in scores:
    for range_string in range_strings:
        bottom_range = int(range_string.split('-')[0])
        top_range = int(range_string.split('-')[1]) + 1
        if station_score[0] in range(bottom_range, top_range):
            score = station_score[0]
            station = station_score[1]
            for i in range(len(scores_json["children"])):
                if scores_json["children"][i]["name"] == range_string:
                    scores_json["children"][i]["children"].append({"name": (station + ': ' + str(score)), "size": score})

f = open('./scores.json', 'w')
f.write(json.dumps(scores_json, indent=2))
f.close()


doc = prov.model.ProvDocument.deserialize('plan.json')

# Provenance information
doc2 = prov.model.ProvDocument()
doc2.add_namespace('alg', 'http://datamechanics.io/algorithm/atelass/')
doc2.add_namespace('dat', 'http://datamechanics.io/data/atelass/')
doc2.add_namespace('ont', 'http://datamechanics.io/ontology#')
doc2.add_namespace('log', 'http://datamechanics.io/log#')

this_script = doc2.agent('alg:scoring', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

# Stations_and_scores provenance information
get_stations_and_scores = doc2.activity('log:a'+str(uuid.uuid4()), stations_and_scores_start_time, stations_and_scores_end_time)
doc2.wasAssociatedWith(get_stations_and_scores, this_script)
doc2.used(get_stations_and_scores, 'dat:crimes', stations_and_scores_start_time)
doc2.used(get_stations_and_scores, 'dat:crimes_and_stations', stations_and_scores_start_time)

stations_and_scores = doc2.entity('dat:stations_and_scores', {prov.model.PROV_LABEL:'Stations and Scores', prov.model.PROV_TYPE:'ont:DataSet'})
doc2.wasAttributedTo(stations_and_scores, this_script)
doc2.wasGeneratedBy(stations_and_scores, get_stations_and_scores, stations_and_scores_end_time)
doc2.wasDerivedFrom(stations_and_scores, 'dat:crimes', get_stations_and_scores)
doc2.wasDerivedFrom(stations_and_scores, 'dat:crimes_and_stations', get_stations_and_scores)

doc.update(doc2)

repo.record(doc.serialize())
open('plan.json', 'w').write(json.dumps(json.loads(doc.serialize()), indent=4))
#print(doc.get_provn())
repo.logout()
