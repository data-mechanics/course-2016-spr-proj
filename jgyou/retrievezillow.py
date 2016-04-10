'''
retrievezillow.py

Retrieves latest month median home and rental index values from Zillow
for available zipcodes in Boston. If Zillow does not have data for given
zipcode, the median value in Boston (derived from the median of neighborhood data)
is substituted in the output data.

note - zillow does have an api for neighborhood statistics,
but appeared to return many blank results (results without a value for zindex)
for several api calls. as a result, the aggregate data from zillow's research website
was used instead.

ZHVI - median home value for a given area, reported monthly including single-family, condos, cooperative homes
http://www.zillow.com/wikipages/What's-the-Zillow-Home-Value-Index/

ZRI - zillow rent index for a given area
http://www.zillow.com/research/zillow-rent-index-methodology-2393/
'''

from bs4 import BeautifulSoup
from urllib import parse, request
from json import loads, dumps, load

import pymongo
import prov.model
import datetime
import uuid
import csv
import codecs
from statistics import median


# codecs - http://stackoverflow.com/questions/18897029/read-csv-file-from-url-into-python-3-x-csv-error-iterator-should-return-str

# (1) zillow property values

def getZillow(allzips, zipquery, neighquery, searchtype):
	
	# list of property results to be appended and dumped into database
	zproperty = []
	zpropertyzips = []

	query1 = zipquery
	response = request.urlopen(query1)
	csvproperty = list(csv.reader(codecs.iterdecode(response, "utf-8")))
	csvproperty_rowheaders = csvproperty[0]
	for i, row in enumerate(csvproperty):
		if i == 0:
			continue
		state = row[csvproperty_rowheaders.index("State")]
		city = row[csvproperty_rowheaders.index("City")]
		zipcode = str(row[csvproperty_rowheaders.index("RegionName")])
		if city == "Boston" and state == "MA":
			zindex = row[csvproperty_rowheaders.index(searchtype)]
			zproperty.append({"zipcode": zipcode, "medianprice": int(zindex)})
			zpropertyzips.append(zipcode)
			#print(row[csvproperty_rowheaders.index("zindex")])

	# find a median zindex value to use in the event the zipcode isn't available
	query3 = neighquery
	response = request.urlopen(query3)
	csvp = list(csv.reader(codecs.iterdecode(response, 'utf-8')))
	csvp_rowheaders = csvproperty[0]
	csvp_zindex = []
	for i, row in enumerate(csvp):
		if i == 0:
			continue
		city = row[csvp_rowheaders.index("City")]
		if city == "Boston":
			zindex = row[csvp_rowheaders.index(searchtype)]
			csvp_zindex.append(int(zindex))

	csvp_median = round(median(csvp_zindex))

	# determine which zipcodes were not found in original query

	nozindex = [j for j in allzips if j not in zpropertyzips]
	
	# add "filler" value accordingly
	for i in nozindex:
		zproperty.append({"zipcode": i, "medianprice": csvp_median})

	return zproperty


def make_provdoc(repo, runids, starttime, endtime):
	provdoc = prov.model.ProvDocument()
	provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
	provdoc.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
	provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
	provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
	provdoc.add_namespace('zillow', 'http://files.zillowstatic.com/research/public') # zillow website

	# activity = invocation of script, agent = script, entity = resource
	# agent
	this_script = provdoc.agent('alg:retrievezillow', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

	# input data
	zipcodelist = provdoc.entity('dat:zipcodes', {prov.model.PROV_LABEL:'Boston Zipcodes', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension': 'json'})
	csvrent_neigh = provdoc.entity('zillow:neighzri', {prov.model.PROV_LABEL:'Zillow ZRI By Neighborhood', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension': 'csv'})
	csvrent_zip = provdoc.entity('zillow:zipzri', {prov.model.PROV_LABEL:'Zillow ZRI By Neighborhood', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension': 'csv'})
	csvproperty_neigh =  provdoc.entity('zillow:neighzhvi', {prov.model.PROV_LABEL:'Zillow ZHVI By Neighborhood', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension': 'csv'})
	csvproperty_zip =  provdoc.entity('zillow:zipzhvi', {prov.model.PROV_LABEL:'Zillow ZHVI By Zipcode', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension': 'csv'})
	zillow_input = [csvrent_neigh, csvrent_zip, csvproperty_neigh, csvproperty_zip]

	# output data
	medianpropertyoutput = provdoc.entity('dat:medianpropertyvalues', {prov.model.PROV_LABEL:'Boston Median Property Values', prov.model.PROV_TYPE:'ont:DataSet'})
	medianrentaloutput = provdoc.entity('dat:medianrentalvalues', {prov.model.PROV_LABEL:'Boston Median Rental Values', prov.model.PROV_TYPE:'ont:DataSet'})


	for i, run_id in enumerate(runids):
		this_run = provdoc.activity('log:a'+run_id, startTime, endTime)
		provdoc.wasAssociatedWith(this_run, this_script)
		provdoc.used(this_run, zillow_input[i])

		output = medianpropertyoutput
		if i < 2:
			output = medianrentaloutput

		provdoc.wasAttributedTo(output, this_script)
		provdoc.wasGeneratedBy(output, this_run)

		provdoc.wasDerivedFrom(output, zillow_input[i])
		provdoc.wasDerivedFrom(output, zipcodelist)


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

			
##########################################################################

exec(open('../pymongo_dm.py').read())

client = pymongo.MongoClient()
repo = client.repo


startTime = datetime.datetime.now()

###########################

f = open("auth.json").read()

auth = loads(f)
user = auth['user']

repo.authenticate(auth['user'], auth['user'])

# get list of zipcodes in Boston
f2 = loads(request.urlopen("http://datamechanics.io/data/jgyou/zipcodes.json").read().decode('utf-8'))

allzips = list(set(["0" + str(d["zipcode"]) for d in f2]))

# create collections for property value, rentals

repo.dropPermanent("medianPropertyValues")
repo.createPermanent("medianPropertyValues")

repo.dropPermanent("medianRentalValues")
repo.createPermanent("medianRentalValues")

# make csv fetches, then insert into collections
zpropertyquery1 = "http://files.zillowstatic.com/research/public/Zip/Zip_Zhvi_Summary_AllHomes.csv"
zpropertyquery2 = "http://files.zillowstatic.com/research/public/Neighborhood/Neighborhood_Zhvi_Summary_AllHomes.csv"
zproperty = getZillow(allzips, zpropertyquery1, zpropertyquery2, "Zhvi")
repo[user + '.medianPropertyValues'].insert_many(zproperty)

zrentalquery1 = "http://files.zillowstatic.com/research/public/Zip/Zip_Zri_AllHomesPlusMultifamily_Summary.csv"
zrentalquery2 = "http://files.zillowstatic.com/research/public/Neighborhood/Neighborhood_Zri_AllHomesPlusMultifamily_Summary.csv"
zrental = getZillow(allzips,zrentalquery1, zrentalquery2, "Zri")
repo[user + '.medianRentalValues'].insert_many(zrental)

endTime = datetime.datetime.now()

##############

runids = [str(uuid.uuid4()) for i in range(4)]

make_provdoc(repo, runids, startTime, endTime)
make_provdoc(repo, runids, None, None)


repo.logout()





