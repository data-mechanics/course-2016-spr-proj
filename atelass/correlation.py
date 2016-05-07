'''
This script goes through the data to see if there's a correlation between number of streetlights and
number of crimes around a given station by calculating the Pearson correlation coefficient.
'''
import pymongo
import time
import prov.model
import json
import datetime
import uuid
import scipy.stats

# Until a library is created, we just use the script directly.
# Path of pymongo_dm.py may need to be changed to ../pymongo_dm.py.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('atelass', 'atelass')

# Get number of crimes and streetlights per station.
print(time.strftime('%I:%M:%S%p') + ': Getting number of crimes and streetlights per station.')
crimes_and_streetlights_per_station_start_time = datetime.datetime.now()

crimes_and_streetlights_per_station = {}
for doc in repo['atelass.crimes_and_stations'].find():
    station = doc['station_info']['stop_name']
    if station not in crimes_and_streetlights_per_station.keys():
        crimes_and_streetlights_per_station[station] = {'num_crimes': 1}
    else:
        crimes_and_streetlights_per_station[station]['num_crimes'] += 1

for doc in repo['atelass.stations_and_streetlights'].find():
    station = doc['station']
    crimes_and_streetlights_per_station[station]['num_streetlights'] = len(doc['streetlight_locations'])

temp = []
for station in crimes_and_streetlights_per_station.keys():
    num_crimes = crimes_and_streetlights_per_station[station]['num_crimes']
    num_streetlights = crimes_and_streetlights_per_station[station]['num_streetlights']
    temp.append({'station': station, 'num_crimes': num_crimes, 'num_streetlights': num_streetlights})
crimes_and_streetlights_per_station_end_time = datetime.datetime.now()

repo.dropPermanent("crimes_and_streetlights_per_station")
repo.createPermanent("crimes_and_streetlights_per_station")
repo['atelass.crimes_and_streetlights_per_station'].insert_many(temp)

print(time.strftime('%I:%M:%S%p') + ': Completed.')

# Calculate Pearson correlation coefficient.
print(time.strftime('%I:%M:%S%p') + ': Analyzing data for correlation.')
# Make tuples in the form of (num_crimes, num_streetlights) for each station.
data = [(crimes_and_streetlights_per_station[station]['num_streetlights'], \
         crimes_and_streetlights_per_station[station]['num_crimes']) \
        for station in crimes_and_streetlights_per_station.keys()]

num_streetlights = [xi for (xi, yi) in data]
num_crimes = [yi for (xi, yi) in data]

correlation_results = scipy.stats.pearsonr(num_streetlights, num_crimes)
print(time.strftime('%I:%M:%S%p') + ': Completed.')
print()

# Print the results.
correlation_cofficient = correlation_results[0]
p_value = correlation_results[1]
print('The Pearson correlation coefficient is: ' + str(correlation_cofficient) + '.')
if correlation_cofficient > 0:
    print('This tells us that there is a positive correlation between the number of streetlights and the number of crimes.')
elif correlation_cofficient < 0:
    print('This tells us that there is a negative correlation between the number of streetlights and the number of crimes.')
else:
    print('This tells us that there does not appear to be any correlation between the number of streetlights and the number of crimes.')
print()
print('The p-value is: ' + str(p_value) + '.')
print('According to documentation[1], "The p-value roughly indicates the probability of an uncorrelated system producing \
datasets that have a Pearson correlation at least as extreme as the one computed from these datasets. The p-values \
are not entirely reliable but are probably reasonable for datasets larger than 500 or so."')
print('\t[1]: http://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.pearsonr.html')


doc = prov.model.ProvDocument.deserialize('plan.json')

# Provenance information
doc2 = prov.model.ProvDocument()
doc2.add_namespace('alg', 'http://datamechanics.io/algorithm/atelass/')
doc2.add_namespace('dat', 'http://datamechanics.io/data/atelass/')
doc2.add_namespace('ont', 'http://datamechanics.io/ontology#')
doc2.add_namespace('log', 'http://datamechanics.io/log#')

this_script = doc2.agent('alg:correlation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

# Crimes_and_streetlights_per_station provenance information
get_crimes_and_streetlights_per_station = doc2.activity('log:a'+str(uuid.uuid4()), crimes_and_streetlights_per_station_start_time, crimes_and_streetlights_per_station_end_time)
doc2.wasAssociatedWith(get_crimes_and_streetlights_per_station, this_script)
doc2.used(get_crimes_and_streetlights_per_station, 'dat:crimes_and_stations', crimes_and_streetlights_per_station_start_time)
doc2.used(get_crimes_and_streetlights_per_station, 'dat:stations_and_streetlights', crimes_and_streetlights_per_station_start_time)

crimes_and_streetlights_per_station = doc2.entity('dat:crimes_and_streetlights_per_station', {prov.model.PROV_LABEL:'Crimes and Streetlights per Station', prov.model.PROV_TYPE:'ont:DataSet'})
doc2.wasAttributedTo(crimes_and_streetlights_per_station, this_script)
doc2.wasGeneratedBy(crimes_and_streetlights_per_station, get_crimes_and_streetlights_per_station, crimes_and_streetlights_per_station_end_time)
doc2.wasDerivedFrom(crimes_and_streetlights_per_station, 'dat:crimes_and_stations', get_crimes_and_streetlights_per_station)
doc2.wasDerivedFrom(crimes_and_streetlights_per_station, 'dat:stations_and_streetlights', get_crimes_and_streetlights_per_station)

doc.update(doc2)

repo.record(doc.serialize())
open('plan.json', 'w').write(json.dumps(json.loads(doc.serialize()), indent=4))
#print(doc.get_provn())
repo.logout()
