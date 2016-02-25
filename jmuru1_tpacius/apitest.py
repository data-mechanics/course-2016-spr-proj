import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid

with open('auth.json') as credentials:
	data = json.load(credentials)

zipcarUrl1 = 'https://data.cityofboston.gov/resource/78f5-5i4e.json' + '?$$app_token=' + data["api_key"] + "&$limit=1000"
zipcarUrl2 = 'https://data.cityofboston.gov/resource/498g-jbmi.json' + '?$$app_token=' + data["api_key"] + "&$limit=1000"
ticketsUrl = 'https://data.cityofboston.gov/resource/cpdb-ie6e.json' + '?$$app_token=' + data["api_key"] + "&$limit=1000"



def request(url):
	response = urllib.request.urlopen(url).read().decode("utf-8")
	r = json.loads(response)
	s = json.dumps(r, sort_keys=True, indent=2)
	return (r , s)

zipCarMemberCount = request(zipcarUrl1)[0]
zipCarReservations = request(zipcarUrl2)[0]
#ticketsByAreaCode = request(ticketsUrl)
