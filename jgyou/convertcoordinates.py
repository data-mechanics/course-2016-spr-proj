from urllib import parse, request
from json import loads, dumps

import pymongo
import prov.model
import datetime
import uuid

#from geopy.geocoders import Nominatim

exec(open('../pymongo_dm.py').read())

# assumes startlocation = x-y, long-lat
# given x-y coordinates, finds approximate address using opencage geocoder

def getAddress(repo, user, startlocation):
  startTime = datetime.datetime.now()
  with open("auth.json") as f:
    auth = json.loads(f.read())
    key = auth['service']['opencagegeo']['key']
    (lat, lon) = startlocation
    # note geocode takes in lat-long
    query = "https://api.opencagedata.com/geocode/v1/json?" + "q=" + "+" + str(lat) \
       + "," + str(lon) + "&pretty=1" + "&countrycode=us" + "&key=" + key

    response = request.urlopen(query).read().decode("utf-8")
    addresults = json.loads(response)

    try: 
      startzip = addresults['results'][0]['components']['postcode']
      neighborhood = addresults['results'][0]['components']['suburb']
      formatted = addresults['results'][0]['formatted']

      endTime = datetime.datetime.now()
      runids = [str(uuid.uuid4())]
      makeProvOpencage(repo, user, runids, startTime, endTime, query)
      makeProvOpencage(repo, user, runids, None, None, query)

      return (formatted, neighborhood, startzip)
    
    except:
      neighborhood = addresults['results'][0]['components']['address100']
      formatted = addresults['results'][0]['formatted']
      return (formatted, neighborhood, -1)


# also takes in long-lat
# returns 15-digit FIPS, the census block
# fips explained: http://www.policymap.com/blog/2012/08/tips-on-fips-a-quick-guide-to-geographic-place-codes-part-iii/
def getCensus(repo, user, startLocation):
  startTime = datetime.datetime.now()
  (lat, lon) = (str(startLocation[0]), str(startLocation[1]))
  query = "http://data.fcc.gov/api/block/find?latitude=" + lat +"&longitude="+ \
    lon +"&showall=true" + "&format=json"

  response = request.urlopen(query).read().decode("utf-8")
  blockresults = json.loads(response)

  # returns 15 digit census block code
  fips = blockresults['Block']['FIPS']

  endTime = datetime.datetime.now()
  runids = [str(uuid.uuid4())]

  makeProvCensus(repo, user, runids, startTime, endTime, query)
  makeProvCensus(repo, user, runids, None, None, query)
  
  return fips


# provenance info
def makeProvCensus(repo, user, runids, startTime, endTime, query):

  repo.authenticate(user, user)

  provdoc = prov.model.ProvDocument()

  provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
  provdoc.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
  provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
  provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
  provdoc.add_namespace('fcc', 'http://data.fcc.gov/api/block/find')

  # activity = invocation of script, agent = script, entity = resource
  # agent
  this_script = provdoc.agent('alg:convertcoordinates', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

  # input data
  inputs = provdoc.entity('fcc:censusblock', {'prov:label':'Census Block Conversion', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

  # output data
  output = provdoc.entity('dat:intermediate', {prov.model.PROV_LABEL:'Intermediate', prov.model.PROV_TYPE:'ont:DataSet'})

  if len(runids) == 1:
    run_id = runids[0]

  this_run = provdoc.activity('log:a'+run_id, startTime, endTime)

  querysuffix = query.split("?")[1]
  provdoc.wasAssociatedWith(this_run, this_script)
  provdoc.used(this_run, inputs, startTime, None,\
    {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'json?' + querysuffix})

  provdoc.wasAttributedTo(output, this_script)
  provdoc.wasGeneratedBy(output, this_run)

  provdoc.wasDerivedFrom(output, inputs)

  if startTime == None:
    plan = open('plan.json','r')
    docModel = prov.model.ProvDocument()
    doc = docModel.deserialize(plan)
    doc.update(provdoc)
    plan.close()
    plan = open('plan.json', 'w')
    plan.write(json.dumps(json.loads(doc.serialize()), indent=4))
    plan.close()
  else:
    repo.record(provdoc.serialize())  
  

def makeProvOpencage(repo, user, runids, startTime, endTime, query):

  repo.authenticate(user, user)

  provdoc = prov.model.ProvDocument()
  provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
  provdoc.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
  provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
  provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
  provdoc.add_namespace('ocd', 'https://api.opencagedata.com/geocode/v1/')

  # activity = invocation of script, agent = script, entity = resource
  # agent
  this_script = provdoc.agent('alg:convertcoordinates', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

  # input data
  opencage = provdoc.entity('ocd:geocode', {'prov:label':'OpenCage Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

  # output data
  output = provdoc.entity('dat:intermediate', {prov.model.PROV_LABEL:'Intermediate', prov.model.PROV_TYPE:'ont:DataSet'})

  if len(runids) == 1:
    run_id = runids[0]

  this_run = provdoc.activity('log:a'+run_id, startTime, endTime)

  querysuffix = query.split("?")[1]
  provdoc.wasAssociatedWith(this_run, this_script)
  provdoc.used(this_run, opencage, startTime, None,\
    {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'json?' + querysuffix})

  provdoc.wasAttributedTo(output, this_script)
  provdoc.wasGeneratedBy(output, this_run)

  provdoc.wasDerivedFrom(output, opencage)

  if startTime == None:
    plan = open('plan.json','r')
    docModel = prov.model.ProvDocument()
    doc = docModel.deserialize(plan)
    doc.update(provdoc)
    plan.close()
    plan = open('plan.json', 'w')
    plan.write(json.dumps(json.loads(doc.serialize()), indent=4))
    plan.close()
  else:
    repo.record(provdoc.serialize())  




'''
sample query
[{'annotations': {'geohash': 'drt2zp3pm2d0w431809u', 'Maidenhead': 'FN42li36bl', 
'OSM': {'edit_url': 'https://www.openstreetmap.org/edit?way=94483071#map=17/42.36041/-71.05797',
 'url': 'https://www.openstreetmap.org/?mlat=42.36041&mlon=-71.05797#map=17/42.36041/-71.05797'},
  'sun': {'rise': {'civil': 1460367480, 'astronomical': 1460363220, 'nautical': 1460365380, 'apparent': 1460369220},
   'set': {'civil': 1460418660, 'astronomical': 1460336580, 'nautical': 1460334360, 'apparent': 1460416920}}
   , 'what3words': {'words': 'pose.rots.cargo'}, 'timezone': {'name': 'America/New_York', 'offset_sec': -14400,
    'now_in_dst': 1, 'offset_string': -400, 'short_name': 'EDT'}, 'DMS': {'lat': "42° 21' 37.47132'' N", 'lng': "71° 3' 28.68761'' W"}, 
    'callingcode': 1, 'Mercator': {'y': 5186322.198, 'x': -7910136.901}, 'MGRS': '19TCG3052591844'}, 
    'components': {'country': 'United States of America', 'country_code': 'us', 'postcode': '02114', 'road': 'Congress Street', 
    'county': 'Suffolk County', 'suburb': 'North End', 'neighbourhood': 'Dock Square', 'city': 'Boston', 'building': 'Boston City Hall',
     'state': 'Massachusetts'}, 'formatted': 'Boston City Hall, Congress Street, Boston, MA 02114, United States of America', 'confidence': 10,
      'geometry': {'lat': 42.3604087, 'lng': -71.0579688}}]
'''

