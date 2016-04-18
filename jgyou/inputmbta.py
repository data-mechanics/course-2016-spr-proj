'''
inputmbta.py

takes stop information from mbta website

saved textfile from mbta GTFS
https://www.mbta.com/uploadedfiles/MBTA_GTFS.zip

'''

from urllib import parse, request
from json import loads, dumps, load

import pymongo
import prov.model
import datetime
import uuid
import zipfile
import io


exec(open('../pymongo_dm.py').read())

def make_provdoc(repo, runids, starttime, endtime):
	provdoc = prov.model.ProvDocument()
	provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
	provdoc.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
	provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
	provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
	provdoc.add_namespace('mbta', 'http://www.mbta.com/uploadedfiles') # mbta website

	# activity = invocation of script, agent = script, entity = resource
	# agent
	this_script = provdoc.agent('alg:inputmbta', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

	# input data
	mbtazip = provdoc.entity('mbta:gtfs', {prov.model.PROV_LABEL:'MBTA_GTFS', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension': 'zip'})

	# output data
	output = provdoc.entity('dat:mbtaStops', {prov.model.PROV_LABEL:'MBTA Stops', prov.model.PROV_TYPE:'ont:DataSet'})

	if len(runids) == 1:
		run_id = runids[0]

	this_run = provdoc.activity('log:a'+run_id, startTime, endTime)
	provdoc.wasAssociatedWith(this_run, this_script)
	provdoc.used(this_run, mbtazip, None, None,{prov.model.PROV_TYPE:'ont:Retrieval', \
		'ont:Query':'MBTA_GTFS.zip'})

	provdoc.wasAttributedTo(output, this_script)
	provdoc.wasGeneratedBy(output, this_run)

	provdoc.wasDerivedFrom(output, mbtazip)

	if starttime == None:
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


client = pymongo.MongoClient()
repo = client.repo

##########


startTime = datetime.datetime.now()


with open("auth.json") as a:
	f = a.read()

	auth = loads(f)
	user = auth['user']

	repo.authenticate(auth['user'], auth['user'])


	with request.urlopen("http://www.mbta.com/uploadedfiles/MBTA_GTFS.zip") as resp:
		response = resp.read()
		with zipfile.ZipFile(io.BytesIO(response)) as z:
			for file in z.namelist():
				if file in ['stops.txt']:
					with z.open(file,"r") as f2:
						mbtatextfile = f2.readlines()
						#mbtatext = request.urlopen("http://datamechanics.io/data/jgyou/stops.txt").read()

						mbtainfo = []

						for stop in mbtatextfile[1:]:
							#print(stop)
							allstr = stop.decode("utf-8").replace('"', "").split(",")
							stop_id = allstr[0]
							stop_name = allstr[2]
							try:
								stop_lat = float(allstr[4])
							except ValueError:
								continue
								#print(allstr[4])
								#stop_lat = "NA"
							try:
								stop_lon = float(allstr[5])	
							except ValueError:
								continue
							temp = allstr[10].strip()
							if str.isdigit(temp):
								wheelchair = int(temp)
							else:
								wheelchair = 0
							mbtainfo.append({"stop_id": stop_id, "stop_name": stop_name, \
								"longitude": stop_lon, "latitude": stop_lat, "wheelchair": wheelchair})

						repo.dropPermanent("mbtaStops")
						repo.createPermanent("mbtaStops")


						repo[user + '.mbtaStops'].insert_many(mbtainfo)


						endTime = datetime.datetime.now()

						run_id = str(uuid.uuid4())
						make_provdoc(repo, [run_id], startTime, endTime)
						make_provdoc(repo, [run_id], None, None)

						repo.logout()








