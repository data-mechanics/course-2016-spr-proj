import prov.model
import pymongo
import datetime
import urllib.request
import time
import uuid

exec(open('../pymongo_dm.py').read())

auth = json.loads(open('auth.json').read())['services']['cityofbostondataportal']

client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('ebrakke_twaltze', 'ebrakke_twaltze')

repo.dropPermanent('crimeReports')
repo.createPermanent('crimeReports')

offset = 0
max_requests = 50000
url = '{}resource/ufcx-3fdn.json?$limit={}&$offset={}&$$app_token={}'

while True:
	this_request = url.format(auth['service'], max_requests, offset, auth['token'])
	print(this_request)
	response = urllib.request.urlopen(this_request).read().decode('utf-8')
	r = json.loads(response)
	if len(r) == 0:
		break
	repo['ebrakke_twaltze.crimeReports'].insert_many(r)
	offset += max_requests
	time.sleep(2)
