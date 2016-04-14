import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid
from geopy.distance import great_circle

# Until a library is created, we just use the script directly.
exec(open('pymongo_dm.py').read())

# Set up the database connection.
auth = open('auth.json', 'r')
cred = json.load(auth)
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate(cred['username'], cred['pwd'])

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

url = 'https://data.cityofboston.gov/resource/7cdf-6fgx.json?incident_type_description=larceny+from+motor+vehicle&$select=location,compnos&$limit=50000'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("larceny")
repo.createPermanent("larceny")
repo['ekwivagg_yuzhou7.larceny'].insert_many(r)

larcenies = repo['ekwivagg_yuzhou7.larceny'].find()

url = 'http://cs-people.bu.edu/sajarvis/datamech/mbta_gtfs/stops.txt'
response = urllib.request.urlopen(url).read().decode("utf-8")
f = open('stops.txt', 'w')
f.write(response)

count = 1
stops = {}
f1 = open('stops.txt', 'r')
for i in range(1, 248):
	line = f1.readline()
	line = line.replace('\"', '')
	if i > 1 and i % 2 == 1 and i < 248:
		stop = line.split(',')
		stops[stop[2]] = [float(stop[4]), float(stop[5])]
	i += 1
#print(stops)

def product(R, S):
    return [(t,u) for t in R for u in S]

def real_aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]

def aggregate(R, f):
    keys = {r[0] for r in R}
    count = 0
    return [(key1, key2, f([v for (k,v) in R if k[0] == key1])) for (key1, key2) in keys]

stop_loc = []
for stop in stops.keys():
	stop_loc.append((stop, stops[stop][0], stops[stop][1]))

count = 0
larceny_loc = []
for larceny in larcenies:
    location = larceny['location']
    latitude = location['latitude']
    longitude = location['longitude']
    larceny_loc.append((count, float(latitude), float(longitude)))
    count += 1
#print(larceny_loc)

larceny_stop = []
for larceny in larceny_loc:
    min_lat = round(larceny[1], 2)
    min_long = round(larceny[2], 2)
    for stop in stop_loc:
        if round(stop[1], 2) == min_lat and round(stop[2], 2) == min_long:
            distance = great_circle((larceny[1], larceny[2]), (stop[1], stop[2])).feet
            larceny_stop.append(((larceny[0], stop[0]), distance))
print(larceny_stop)

closest_stop = aggregate(larceny_stop, min)
closest_t_stop = []
for pair in closest_stop:
    closest_t_stop.append({'larceny': pair[0], 'tstop': pair[1], 'distance':pair[2]})
#print(closest_t_stop)
repo.dropPermanent("closest_stop_larceny")
repo.createPermanent("closest_stop_larceny")
repo['ekwivagg_yuzhou7.closest_stop_larceny'].insert_many(closest_t_stop)

closest_stop_larceny = repo['ekwivagg_yuzhou7.closest_stop_larceny'].find()

X = []
for pair in closest_stop_larceny:
	X.append((pair['tstop'], 1))

Y = real_aggregate(X, sum)
print(Y)

endTime = datetime.datetime.now()

# Provenance information for plan.jason
doc = prov.model.ProvDocument()

doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ekwivagg_yuzhou7/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/ekwivagg_yuzhou7/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
doc.add_namespace('sj', 'http://cs-people.bu.edu/sajarvis/datamech/mbta_gtfs/')

this_script = doc.agent('alg: get_crime', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

larceny_dat = doc.entity('bdp:7cdf-6fgx', {prov.model.PROV_LABEL:'Larcenies', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
stops = doc.entity('sj:stops', {'prov:label':'T Stops', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'txt'})
closest_stop_larceny = doc.entity('dat:closest_stop_larceny', {prov.model.PROV_LABEL:'Closest Larceny Stop', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
crime_freq = doc.entity('dat:crime_freq', {prov.model.PROV_LABEL:'Crime Frequency', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})

stop_retrieval = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})
doc.wasAssociatedWith(stop_retrieval, this_script)
doc.used(stop_retrieval, stops, startTime)

larceny_retrieval = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?incident_type_description=larceny+from+motor+vehicle&$select=location,compnos&$limit=50000'})
doc.wasAssociatedWith(larceny_retrieval, this_script)
doc.used(larceny_retrieval, larceny_dat, startTime)

closest_larceny_calc = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
doc.wasAssociatedWith(closest_liquor_calc, this_script)
doc.used(closest_larceny_calc, larceny_dat, startTime)
doc.used(closest_larceny_calc, stops, startTime)

get_frequency = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
doc.wasAssociatedWith(get_frequency, this_script)
doc.used(restaurant_freq, closest_stop_larceny, startTime)


doc.wasAttributedTo(closest_stop_larceny, this_script)
doc.wasGeneratedBy(closest_stop_larceny, closest_larceny_calc, endTime)
doc.wasDerivedFrom(closest_stop_larceny, larceny_dat, closest_larceny_calc, closest_larceny_calc, closest_larceny_calc)
doc.wasDerivedFrom(closest_stop_larceny, stops, closest_larceny_calc, closest_larceny_calc, closest_larceny_calc)
doc.wasDerivedFrom(closest_stop_larceny, crime_freq, get_frequency, get_frequency, get_frequency)

repo.record(doc.serialize())
content = json.dumps(json.loads(doc.serialize()), indent=4)
f = open('plan.json', 'a')
f.write(",\n")
f.write(content)
repo.logout()
