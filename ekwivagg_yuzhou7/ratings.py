import urllib.request
import json
import csv
import pymongo
import prov.model
import datetime
import uuid
from collections import defaultdict

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

ratings = {}

restaurants = repo['ekwivagg_yuzhou7.restaurant'].find()
closest_stops = repo['ekwivagg_yuzhou7.closest_stop'].find()
crime_freqs = repo['ekwivagg_yuzhou7.crime_freq'].find()
rodent_problems = repo['ekwivagg_yuzhou7.rodent_problem'].find()
inspection_failures = repo['ekwivagg_yuzhou7.inspection_failures'].find()

#distance = []
crime = {}
for t in crime_freqs:
	for key in t.keys():
		if key != '_id':
			crime[key] = t[key]
rats = {}
for rat in rodent_problems:
	#businessnames = []
	for key in rat:
		if key != '_id':
			for name in rat[key]['businesses']:
				rats[name] = rat[key]['rodent_problem']

fails = {}
for f in inspection_failures:
	for key in f.keys():
		if key != '_id':
			fails[key] = f[key]

for stop in closest_stops:
	restaurant = stop['restaurant']
	tstop = stop['tstop']

	if stop['distance'] <= 200:
		ratings[restaurant] = [3.0]
		#three += 1
	elif stop['distance'] <= 700:
		ratings[restaurant] = [2.0]
		#two += 1
	else:
		ratings[restaurant] = [1.0]
		#one += 1

	if not tstop in crime:
		ratings[restaurant].append(1.0)
	else:
		if crime[tstop] <= 60:
			ratings[restaurant].append(1.0)
		else:
			ratings[restaurant].append(0.5)

	if rats[restaurant]:
		ratings[restaurant].append(0.0)
	else:
		ratings[restaurant].append(0.5)

	if restaurant in fails and fails[restaurant]:
		ratings[restaurant].append(0.0)
	else:
		ratings[restaurant].append(0.5)

allr = []
for key in ratings:
	allr.append(sum(ratings[key]))


print (len(allr))

maxd = max(allr)
print('max', maxd)
mind = min(allr)
print('min', mind)
sumd = sum(allr)
avg = sumd/len(allr)
print('avg', avg)

'''with open('ratings.csv', 'w', newline='') as out:
    fieldnames = ['Restaurant', 'Distance', 'Crime', 'Rodent Problem', 'Inspectons']
    dw = csv.DictWriter(out, fieldnames=fieldnames)
    dw.writeheader()
    for key in ratings.keys():
        dw.writerow({'Restaurant': key, 'Distance': ratings[key][0], 'Crime':ratings[key][1], 'Rodent Problem':ratings[key][2], 'Inspectons':ratings[key][3]})'''

closest_stops = repo['ekwivagg_yuzhou7.closest_stop'].find()
avg_rating = defaultdict(list)
for stop in closest_stops:
	restaurant = stop['restaurant']
	tstop = stop['tstop']

	rating = sum(ratings[restaurant])
	#print (rating)
	avg_rating[tstop].append(rating)

'''with open('avg_rating.csv', 'w', newline='') as out:
    fieldnames = ['T_Stop', 'Average_Rating']
    dw = csv.DictWriter(out, fieldnames=fieldnames)
    dw.writeheader()
    for key in avg_rating.keys():
        dw.writerow({'T_Stop': key, 'Average_Rating': sum(avg_rating[key])/len(avg_rating[key])})'''


blue = ['Wonderland', 'Revere Beach', 'Beachmont', 'Suffolk Downs', 'Orient Heights', 'Wood Island', 'Airport', 'Maverick', 'Aquarium', 'State', 'Government Center', 'Bowdoin']
greenb = ['Park Street', 'Boylston', 'Arlington', 'Copley', 'Hynes Convention Center', 'Kenmore', 'Blanford', 'Boston Univ. East', 'Boston Univ. Central', 'Boston Univ. West', 'Saint Paul', 'Pleasant Street', 'Babcock Street', 'Packards Corner', 'Harvard Ave.', 'Griggs Street', 'Allston Street', 'Warren Street', 'Washington Street', 'Sutherland Road', 'Chiswick Road', 'Chestnut Hill Ave.', 'South Street', 'Boston College']
greenc = ['North Station', 'Haymarket', 'Government Center', 'Park Street', 'Boylston', 'Arlington', 'Copley', 'Hynes Convention Center', 'Kenmore', 'Saint Marys Street', 'Hawes Street', 'Kent Street', 'Saint Paul', 'Coolidge Corner', 'Summit Ave.', 'Brandon Hall', 'Fairbanks', 'Washington Square', 'Tappan Street', 'Dean Road', 'Englewood Ave.', 'Cleveland Circle']
greend = ['Park Street', 'Boylston', 'Arlington', 'Copley', 'Hynes Convention Center', 'Kenmore', 'Fenway', 'Longwood', 'Brookline Village', 'Brookline Hills', 'Beaconsfield', 'Reservoir', 'Chestnut Hill', 'Newton Centre', 'Newton Highlands', 'Eliot', 'Waban', 'Woodland', 'Riverside']
greene = ['Lechmere', 'Science Park', 'North Station', 'Haymarket', 'Government Center', 'Park Street', 'Boylston', 'Arlington', 'Copley', 'Hynes Convention Center', 'Kenmore', 'Prudential', 'Symphony', 'Northeastern University', 'Museum of Fine Arts', 'Longwood Medical Area', 'Brigham Circle', 'Fenwood Road', 'Mission Park', 'Riverway', 'Back of the Hill', 'Heath Street']
red = ['Alewife Station', 'Davis Station', 'Porter Square', 'Harvard Square', 'Central Square', 'Kendall Square', 'Charles/MGH', 'Park Street', 'Downtown Crossing', 'South Station', 'Broadway', 'Andrew', 'JFK/UMass', 'North Quincy', 'Wollaston', 'Quincy Center', 'Quincy Adams', 'Braintree', 'Savin Hill', 'Fields Corner', 'Shawmut', 'Ashmont', 'Cedar Grove', 'Butler', 'Milton', 'Central Ave.', 'Valley Road', 'Capen Street', 'Mattapan']
orange = ['Oak Grove', 'Malden Center', 'Wellington', 'Assembly', 'Sullivan Square', 'Community College', 'North Station', 'Haymarket', 'State', 'Downtown Crossing', 'Chinatown', 'Tufts Medical Center', 'Back Bay', 'Massachusetts Ave.', 'Ruggles', 'Roxbury Crossing', 'Jackson Square', 'Stony Brook', 'Green Street', 'Forest Hills']

f = open('avg_rating.json', 'w')

data = []
for key in avg_rating.keys():
	item = {}
	item['t_stop'] = key
	item['avg_rating'] = sum(avg_rating[key])/len(avg_rating[key])
	data.append(item)
json.dump(data, f)

data = []
blue_line = []
green_line_b = []
green_line_c = []
green_line_d = []
green_line_e = []
red_line = []
orange_line = []
for key in avg_rating.keys():
	item = {}
	t_stop = key
	rating = sum(avg_rating[key])/len(avg_rating[key])
	if t_stop in blue:
		blue_line.append(rating)

	if t_stop in greenb:
		green_line_b.append(rating)

	if t_stop in greenc:
		green_line_c.append(rating)

	if t_stop in greend:
		green_line_d.append(rating)

	if t_stop in greend:
		green_line_e.append(rating)

	if t_stop in red:
		red_line.append(rating)

	if t_stop in orange:
		orange_line.append(rating)

blue = {}
blue['line'] = 'blue'
blue['avg'] = sum(blue_line)/len(blue_line)
data.append(blue)

b = {}
b['line'] = 'b'
b['avg'] = sum(green_line_b)/len(green_line_b)
data.append(b)

c = {}
c['line'] = 'c'
c['avg'] = sum(green_line_c)/len(green_line_c)
data.append(c)

d ={}
d['line'] = 'd'
d['avg'] = sum(green_line_d)/len(green_line_d)
data.append(d)

e = {}
e['line'] = 'e'
e['avg'] = sum(green_line_e)/len(green_line_e)
data.append(e)

red = {}
red['line'] = 'red'
red['avg'] = sum(red_line)/len(red_line)
data.append(red)

orange = {}
orange['line'] = 'orange'
orange['avg'] = sum(orange_line)/len(orange_line)
data.append(orange)

f2 = open('avg_rating_line.json', 'w')
json.dump(data, f2)

endTime = datetime.datetime.now()

# Provenance information for plan.jason
doc = prov.model.ProvDocument()

doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ekwivagg_yuzhou7/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/ekwivagg_yuzhou7/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
doc.add_namespace('sj', 'http://cs-people.bu.edu/sajarvis/datamech/mbta_gtfs/')

this_script = doc.agent('alg: ratings', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

crime_freqs = doc.entity('dat:crime_freq', {prov.model.PROV_LABEL:'Larcenies', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
rodent_problems = doc.entity('dat:rodent_problem', {prov.model.PROV_LABEL:'Rodent Problems', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
inspection_failures = doc.entity('dat:inspection_failures', {prov.model.PROV_LABEL:'Inspection Failures', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
restaurants = doc.entity('dat:restaurant', {prov.model.PROV_LABEL:'Restaurants', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
closest_stops = doc.entity('dat:closest_stops', {prov.model.PROV_LABEL:'Closest Stops', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

doc.wasAssociatedWith(this_run, this_script)

doc.used(this_run, restaurants, startTime)
doc.used(this_run, crime_freqs, startTime)
doc.used(this_run, rodent_problems, startTime)
doc.used(this_run, inspection_failures, startTime)
doc.used(this_run, closest_stops, startTime)

repo.record(doc.serialize())
content = json.dumps(json.loads(doc.serialize()), indent=4)
f = open('plan.json', 'a')
f.write(",\n")
f.write(content)
repo.logout()