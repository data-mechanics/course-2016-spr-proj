import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid

with open('auth.json') as credentials:
	data = json.load(credentials)

propertyUrl = 'https://data.cityofboston.gov/resource/n7za-nsjh.json'  + '?$$app_token=' + data["api_key"] + "&$limit=1000"
propertyUrl2 = 'https://data.cityofboston.gov/resource/n7za-nsjh.json'  + '?$$app_token=' + data["api_key"] + "&$limit=16000"
streetJamsUrl = 'https://data.cityofboston.gov/resource/yqgx-2ktq.json?'
hospitalsUrl = "https://data.cityofboston.gov/resource/46f7-2snz.json?"
emsDepartureUrl= "https://data.cityofboston.gov/resource/ak6k-a5up.json?"

def request(url):
	response = urllib.request.urlopen(url).read().decode("utf-8")
	r = json.loads(response)
	s = json.dumps(r, sort_keys=True, indent=2)
	return (r , s)

propertyvalue = request(propertyUrl)[0]
propertyvalue2 = request(propertyUrl2)[0]
streetJams = request(streetJamsUrl)[0]
hospitals = request(hospitalsUrl)[0]
emsDeparture = request(emsDepartureUrl)[0]
#
# print(len(propertyvalue2))
#eof
