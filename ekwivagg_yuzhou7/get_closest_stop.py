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

restaurants = repo['ekwivagg_yuzhou7.restaurant'].find()

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

def aggregate(R, f):
    keys = {r[0] for r in R}
    count = 0
    return [(key1, key2, f([v for (k,v) in R if k[0] == key1])) for (key1, key2) in keys]

stop_loc = []
for stop in stops.keys():
	stop_loc.append((stop, stops[stop][0], stops[stop][1]))

restaurant_loc = []
for restaurant in restaurants:
    location = restaurant['location']
    latitude = location['latitude']
    longitude = location['longitude']
    restaurant_loc.append((restaurant['businessname'], float(latitude), float(longitude)))


#restaurant_stop = [((r, s), great_circle((rla, rlo), (sla, slo)).feet) for ((r, rla, rlo), (s, sla, slo)) in product(restaurant_loc, stop_loc)]
#print(len(restaurant_stop))
restaurant_stop = []
for restaurant in restaurant_loc:
    min_lat = round(restaurant[1], 2)
    min_long = round(restaurant[2], 2)
    for stop in stop_loc:
        if round(stop[1], 2) == min_lat and round(stop[2], 2) == min_long:
            distance = great_circle((restaurant[1], restaurant[2]), (stop[1], stop[2])).feet
            restaurant_stop.append(((restaurant[0], stop[0]), distance))
print(len(restaurant_stop))


closest_stop = aggregate(restaurant_stop, min)
closest_t_stop = []
for pair in closest_stop:
    closest_t_stop.append({'restaurant': pair[0], 'tstop': pair[1], 'distance':pair[2]})
repo.dropPermanent("closest_stop")
repo.createPermanent("closest_stop")
repo['ekwivagg_yuzhou7.closest_stop'].insert_many(closest_t_stop)

endTime = datetime.datetime.now()

doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ekwivagg_yuzhou7/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/ekwivagg_yuzhou7/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
doc.add_namespace('sj', 'http://cs-people.bu.edu/sajarvis/datamech/mbta_gtfs/')

this_script = doc.agent('alg:closest_stop', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

restaurant_dat = doc.entity('dat:restaurant', {prov.model.PROV_LABEL:'Restaurants', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
restaurant_retrieval = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})
doc.wasAssociatedWith(restaurant_retrieval, this_script)
doc.used(restaurant_retrieval, restaurant_dat, startTime)


stops = doc.entity('sj:stops', {'prov:label':'T Stops', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'txts'})
this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})
doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, stops, startTime)


closest_stop_calc = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
doc.wasAssociatedWith(closest_stop_calc, this_script)
doc.used(closest_stop_calc, restaurant_dat, startTime)
doc.used(closest_stop_calc, stops, startTime)

closest_stop = doc.entity('dat:closest_stop', {prov.model.PROV_LABEL:'Closest T Stop', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
doc.wasAttributedTo(closest_stop, this_script)
doc.wasGeneratedBy(closest_stop, closest_stop_calc, endTime)
doc.wasDerivedFrom(closest_stop, restaurant_dat, closest_stop_calc, closest_stop_calc, closest_stop_calc)
doc.wasDerivedFrom(closest_stop, stops, closest_stop_calc, closest_stop_calc, closest_stop_calc)

repo.record(doc.serialize())
content = json.dumps(json.loads(doc.serialize()), indent=4)
f = open('plan.json', 'a')
f.write(",\n")
f.write(content)
repo.logout()