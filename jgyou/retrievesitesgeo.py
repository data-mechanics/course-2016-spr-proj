'''
retrievesitesgeo.py

This script takes street addresses from the drop-off sites data
to find geographic data from Opencage Data. 
(This site uses geocoding to convert addresses to longitude and latitude).
'''

from urllib import parse, request
from json import loads, dumps

import pymongo
import prov.model
import datetime
import uuid

exec(open('../pymongo_dm.py').read())

client = pymongo.MongoClient()
repo = client.repo

##########


startTime = datetime.datetime.now()

f = open("auth.json").read()

auth = loads(f)
user = auth['user']
# remember to modify this line later
repo.authenticate(auth['user'], auth['user'])


# authorization key
key = auth['service']['opencagegeo']['key'] 

repo.dropPermanent("sitegeocodes")
repo.createPermanent("sitegeocodes")


# loop through collection of current sites and make api calls based on information in each document
param = []

for site in repo[user + '.currentsites'].find():	
	s = site['location_street_name'] + "," + site['neighborhood'] + ", MA" + " " + site['location_zipcode']
	param.append(s)

	query = "https://api.opencagedata.com/geocode/v1/geojson?q=" + parse.quote_plus(s) + "&limit=1" +\
	 "&pretty=1" + "&countrycode=us" +"&key=" + key
	georesult =  request.urlopen(query).read().decode("utf-8")
	geojs = json.loads(georesult)

	# tie together site id with coordinate data by adding a field to json
	geojs['siteid'] = str(site['_id'])
	georesult = dumps(geojs)

	# then insert coordinates into new collection
	repo[user + '.sitegeocodes'].insert(loads(georesult))


###########

endTime = datetime.datetime.now()


# record provenance data

provdoc = prov.model.ProvDocument()
provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
provdoc.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
provdoc.add_namespace('ocd', 'https://api.opencagedata.com/geocode/v1/')

# activity = invocation of script, agent = script, entity = resource
# agent
this_script = provdoc.agent('alg:retrievesitesgeo', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

# input data
dropoffsites = provdoc.entity('dat:currentsites', {prov.model.PROV_LABEL:'Current Drop-Off Sites', prov.model.PROV_TYPE:'ont:DataSet'})
resource = provdoc.entity('ocd:geocode', {'prov:label':'OpenCage Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})

# output dataset
sitecodesoutput = provdoc.entity('dat:sitegeocodes', {prov.model.PROV_LABEL:'Site Geocodes', prov.model.PROV_TYPE:'ont:DataSet'})

provdoc2 = prov.model.ProvDocument()
provdoc2.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
provdoc2.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
provdoc2.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
provdoc2.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
provdoc2.add_namespace('ocd', 'https://api.opencagedata.com/geocode/v1/')

# activity = invocation of script, agent = script, entity = resource
# agent
this_script2 = provdoc2.agent('alg:retrievesitesgeo', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

# input data
dropoffsites2 = provdoc2.entity('dat:currentsites', {prov.model.PROV_LABEL:'Current Drop-Off Sites', prov.model.PROV_TYPE:'ont:DataSet'})
resource2 = provdoc2.entity('ocd:geocode', {'prov:label':'OpenCage Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})

# output dataset
sitecodesoutput2 = provdoc2.entity('dat:sitegeocodes', {prov.model.PROV_LABEL:'Site Geocodes', prov.model.PROV_TYPE:'ont:DataSet'})

# record provenance for each geocode query
for i in param:
	runid = str(uuid.uuid4())
	this_run = provdoc.activity('log:a'+run_id, startTime, endTime)

	provdoc.wasAssociatedWith(this_run, this_script)
	
	provdoc.used(this_run, resource, startTime, None,\
		{prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'geojson?q=' + i + '&limit=1&countrycode=us'})
	provdoc.used(this_run, dropoffsites, startTime)
	
	provdoc.wasGeneratedBy(sitecodesoutput, this_run, endTime)

	provdoc.wasDerivedFrom(sitecodesoutput, resource, this_run, this_run, this_run)
	provdoc.wasDerivedFrom(sitecodesoutput, dropoffsites, this_run, this_run, this_run)
	provdoc.wasAttributedTo(sitecodesoutput, this_script)

	this_run2 = provdoc2.activity('log:a'+run_id)

	provdoc2.wasAssociatedWith(this_run2, this_script2)
	
	provdoc2.used(this_run2, resource2, None, None,\
		{prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'geojson?q=' + i + '&limit=1&countrycode=us'})
	provdoc2.used(this_run2, dropoffsites2)
	
	provdoc2.wasGeneratedBy(sitecodesoutput2, this_run2)

	provdoc2.wasDerivedFrom(sitecodesoutput2, resource2, this_run2, this_run2, this_run2)
	provdoc2.wasDerivedFrom(sitecodesoutput2, dropoffsites2, this_run2, this_run2, this_run2)
	provdoc2.wasAttributedTo(sitecodesoutput2, this_script2)


repo.record(provdoc.serialize()) # Record the provenance document.

plan = open('plan.json','r')
docModel = prov.model.ProvDocument()
doc2 = docModel.deserialize(plan)
doc2.update(provdoc2)
plan.close()
plan = open('plan.json', 'w')
plan.write(json.dumps(json.loads(doc2.serialize()), indent=4))
plan.close()

repo.logout()








