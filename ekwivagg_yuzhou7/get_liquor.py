import urllib.request
import json
import csv
import dml
import prov.model
import datetime
import uuid
from geopy.distance import great_circle

# Set up the database connection.
teamname = 'ekwivagg_yuzhou7'
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate(teamname, teamname)

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

restaurants = repo['ekwivagg_yuzhou7.restaurant'].find()

url = 'http://cs-people.bu.edu/sajarvis/datamech/mbta_gtfs/stops.txt'
response = urllib.request.urlopen(url).read().decode("utf-8")
f = open('stops.txt', 'w')
f.write(response)

url = 'https://data.cityofboston.gov/resource/hda6-fnsh.json?$select=dbaname&$limit=50000'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("liquor")
repo.createPermanent("liquor")
repo['ekwivagg_yuzhou7.liquor'].insert_many(r)

liquors = repo['ekwivagg_yuzhou7.liquor'].find()

stops = {}
f1 = open('stops.txt', 'r')
for i in range(1, 248):
	line = f1.readline()
	line = line.replace('\"', '')
	if i > 1 and i % 2 == 1 and i < 248:
		stop = line.split(',')
		stops[stop[2]] = [float(stop[4]), float(stop[5])]
	i += 1

stop_loc = []
for stop in stops.keys():
	stop_loc.append((stop, stops[stop][0], stops[stop][1]))

restaurant_loc = []
for restaurant in restaurants:
    location = restaurant['location']
    latitude = location['latitude']
    longitude = location['longitude']
    restaurant_loc.append((restaurant['businessname'], float(latitude), float(longitude)))

liq_names = []
for liquor in liquors:
	if '\xc9' in liquor['dbaname']:
		name = liquor['dbaname'].replace('\xc9', 'e')
	else:
		name = liquor['dbaname']
	liq_names.append(name)

def product(R, S):
    return [(t,u) for t in R for u in S]

def real_aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]

def aggregate(R, f):
    keys = {r[0] for r in R}
    count = 0
    return [(key1, key2, f([v for (k,v) in R if k[0] == key1])) for (key1, key2) in keys]

restaurant_liq = [(r, rlat, rlong) for ((r, rlat, rlong), (liq)) in product(restaurant_loc, liq_names) if r == liq]
#print(restaurant_liq)

restaurant_liq_stop = []
for restaurant in restaurant_liq:
    min_lat = round(restaurant[1], 2)
    min_long = round(restaurant[2], 2)
    for stop in stop_loc:
        if round(stop[1], 2) == min_lat and round(stop[2], 2) == min_long:
            distance = great_circle((restaurant[1], restaurant[2]), (stop[1], stop[2])).feet
            restaurant_liq_stop.append(((restaurant[0], stop[0]), distance))
#print(restaurant_liq_stop)

closest_stop = aggregate(restaurant_liq_stop, min)
closest_t_stop = []
for pair in closest_stop:
    closest_t_stop.append({'liq': pair[0], 'tstop': pair[1], 'distance':pair[2]})
#print(closest_t_stop)
repo.dropPermanent("closest_stop_liq")
repo.createPermanent("closest_stop_liq")
repo['ekwivagg_yuzhou7.closest_stop_liq'].insert_many(closest_t_stop)

closest_stop_liq = repo['ekwivagg_yuzhou7.closest_stop_liq'].find()

X = []
for pair in closest_stop_liq:
	X.append((pair['tstop'], 1))

Y = real_aggregate(X, sum)
#print(Y)

liquor_freq = []
for i in Y:
    if '.' in i[0]:
        name = i[0].replace('.', '')
    else:
        name = i[0]
    liquor_freq.append({name:i[1]})
#print(liquor_freq)
repo.dropPermanent("liquor_freq")
repo.createPermanent("liquor_freq")
repo['ekwivagg_yuzhou7.liquor_freq'].insert_many(liquor_freq)

liquor_freq_one = {}
for i in liquor_freq:
    for key in i.keys():
        liquor_freq_one[key] = i[key]
liquor_freq = [liquor_freq_one]

with open('liquor.tsv', 'w') as out:
    dw = csv.DictWriter(out, sorted(liquor_freq[0].keys()), delimiter='\t')
    dw.writeheader()
    dw.writerows(liquor_freq)

endTime = datetime.datetime.now()

# Provenance information for plan.jason
doc = prov.model.ProvDocument()

doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ekwivagg_yuzhou7/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/ekwivagg_yuzhou7/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
doc.add_namespace('sj', 'http://cs-people.bu.edu/sajarvis/datamech/mbta_gtfs/')

this_script = doc.agent('alg: get_liquor', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

restaurant_dat = doc.entity('dat:restaurant', {prov.model.PROV_LABEL:'Restaurants', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
liquor_dat = doc.entity('bdp:hda6-fnsh', {prov.model.PROV_LABEL:'Liquor', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
stops = doc.entity('sj:stops', {'prov:label':'T Stops', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'txts'})
closest_stop_liq = doc.entity('dat:closest_stop_liq', {prov.model.PROV_LABEL:'Closest Liquor Stop', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
liquor_freq = doc.entity('dat:liquor_freq', {prov.model.PROV_LABEL:'Liquor Frequency', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})

restaurant_retrieval = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})
doc.wasAssociatedWith(restaurant_retrieval, this_script)
doc.used(restaurant_retrieval, restaurant_dat, startTime)

stop_retrieval = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})
doc.wasAssociatedWith(stop_retrieval, this_script)
doc.used(stop_retrieval, stops, startTime)

liquor_retrieval = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Guery':'?$select=dbaname&$limit=50000'})
doc.wasAssociatedWith(liquor_retrieval, this_script)
doc.used(liquor_retrieval, liquor_dat, startTime)

closest_liquor_calc = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
doc.wasAssociatedWith(closest_liquor_calc, this_script)
doc.used(closest_liquor_calc, restaurant_dat, startTime)
doc.used(closest_liquor_calc, stops, startTime)
doc.used(closest_liquor_calc, liquor_dat, startTime)

get_frequency = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
doc.wasAssociatedWith(get_frequency, this_script)
doc.used(get_frequency, closest_stop_liq, startTime)

doc.wasAttributedTo(closest_stop_liq, this_script)
doc.wasGeneratedBy(closest_stop_liq, closest_liquor_calc, endTime)
doc.wasDerivedFrom(closest_stop_liq, restaurant_dat, closest_liquor_calc, closest_liquor_calc, closest_liquor_calc)
doc.wasDerivedFrom(closest_stop_liq, stops, closest_liquor_calc, closest_liquor_calc, closest_liquor_calc)
doc.wasDerivedFrom(closest_stop_liq, liquor_dat, closest_liquor_calc, closest_liquor_calc, closest_liquor_calc)
doc.wasDerivedFrom(liquor_freq, closest_stop_liq, get_frequency, get_frequency, get_frequency)

repo.record(doc.serialize())
content = json.dumps(json.loads(doc.serialize()), indent=4)
f = open('plan.json', 'a')
f.write(",\n")
f.write(content)
repo.logout()
