import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid

with open('auth.json') as credentials:
	data = json.load(credentials)

zipcarUrl1 = 'https://data.cityofboston.gov/resource/78f5-5i4e.json' + '?$$app_token=' + data["api_key"] + "&$limit=10"
zipcarUrl2 = 'https://data.cityofboston.gov/resource/498g-jbmi.json' + '?$$app_token=' + data["api_key"]# + "&$limit=10"
ticketsUrl = 'https://data.cityofboston.gov/resource/cpdb-ie6e.json' + '?$$app_token=' + data["api_key"] #+ "&$limit=10"



def request(url):
	response = urllib.request.urlopen(url).read().decode("utf-8")
	r = json.loads(response)
	s = json.dumps(r, sort_keys=True, indent=2)
	return (r , s)

#print(request(zipcarUrl1))
#print(zipcarUrl2)
# print(request(ticketsUrl))