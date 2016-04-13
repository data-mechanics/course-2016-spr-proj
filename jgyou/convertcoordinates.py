from urllib import parse, request
from json import loads, dumps

import pymongo
import prov.model
import datetime
import uuid

exec(open('../pymongo_dm.py').read())

# assumes startlocation = x-y, long-lat
# given x-y coordinates, finds approximate address using opencage geocoder
def getAddress(startlocation):
  # note geocode takes in lat-long
  query = "https://api.opencagedata.com/geocode/v1/json?" + "q=" + str(startlocation[1]) \
  	 + "," + "+" + str(startlocation[0]) + "&pretty=1" + "&countrycode=us" + "&key=" + key

  response = request.urlopen(query).read().decode("utf-8")
  addresults = json.loads(response)

  startzip = addresults['results'][0]['components']['postcode']
  neighborhood = addresults['results'][0]['components']['suburb']
  formatted = addresults['results'][0]['formatted']

  return (formatted, neighborhood, startzip)


# also takes in long-lat
# returns 15-digit FIPS, the census block
# fips explained: http://www.policymap.com/blog/2012/08/tips-on-fips-a-quick-guide-to-geographic-place-codes-part-iii/
def getCensus(startLocation):
  (lon,lat) = (str(startLocation[0]), str(startLocation[1]))
  query = "http://data.fcc.gov/api/block/find?latitude=" + lat +"&longitude="+ \
    lon +"&showall=true" + "&format=json"

  response = request.urlopen(query).read().decode("utf-8")
  blockresults = json.loads(response)

  # returns 15 digit census block code
  fips = blockresults['Block']['FIPS']
  
  return fips


# provenance info
def makeProv():
  return  




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


# client = pymongo.MongoClient()
# repo = client.repo

# ##########


# startTime = datetime.datetime.now()

# f = open("auth.json").read()

# auth = loads(f)
# user = auth['user']

# repo.authenticate(auth['user'], auth['user'])

# # authorization key
# key = auth['service']['opencagegeo']['key'] 

# listed as x-y, long-lat
