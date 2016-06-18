'''
retrievepharma.py

This script retrieves pharmacies from Yelp by zip code.
'''
from urllib import request, parse
import json
import dml
import prov.model
import datetime
import uuid
import time
from yelp.client import Client
from yelp.oauth1_authenticator import Oauth1Authenticator

# set up connection

client = dml.pymongo.MongoClient()
repo = client.repo

f = open("auth.json").read()
auth = json.loads(f)
user = auth['user']

repo.authenticate(user, user)

####################################

startTime = datetime.datetime.now()

# load up zip codes

zipcodes = request.urlopen("https://data-mechanics.s3.amazonaws.com/jgyou/zipcodes.json").read().decode("utf-8")
ziplist = json.loads(zipcodes)

# yelp setup

ckey = auth['service']['yelp']['consumer_key']
csecret = auth['service']['yelp']['consumer_secret']
ytoken = auth['service']['yelp']['token']
ytokensecret = auth['service']['yelp']['token_secret']

yelpAuth = Oauth1Authenticator(
    consumer_key=ckey,
    consumer_secret=csecret,
    token=ytoken,
    token_secret=ytokensecret
)

yelpClient = Client(yelpAuth)

repo.dropPermanent("pharmacy")
repo.createPermanent("pharmacy")

params= {
		'term': 'pharmacy',
		'location': "02115"
	}

R = yelpClient.search(**params)

# store businesses in 

pharm = []

for r in R.businesses:
	pharm_name = r.name
	pharm_lat = r.location.coordinate.latitude
	pharm_lon = r.location.coordinate.longitude
	pharm_zipcode = r.location.postal_code
	pharm_city = r.location.city
	pharm_neigh = r.location.neighborhoods # multiple possible
	pharm_street = r.location.address[0]
	pharm_fulladdress = ", ".join(r.location.display_address)
	pharm_phone = r.phone
	pharm.append({"name": pharm_name, "latitude": pharm_lat, "longitude": pharm_lon, \
	 	"location_zipcode": pharm_zipcode, "town": pharm_city, "neighborhood": pharm_neigh, \
	  	"street": pharm_street, "address": pharm_fulladdress, "phone": r.phone})

repo[user + '.pharmacy'].insert_many((pharm))

###########

endTime = datetime.datetime.now()

'''
for z in ziplist:

	params= {
		'term': 'pharmacy',
		'location': "0" + str(z['zipcode'])
	}

	R = yelpClient.search('Boston', **params)

	for r in R:
		repo[user + '.pharmacy'].insert(json.loads(r))

'''

repo.logout()

