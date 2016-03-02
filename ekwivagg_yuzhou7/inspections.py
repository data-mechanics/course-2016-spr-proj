import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
auth = open('auth.json', 'r')
cred = json.load(auth)
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate(cred['username'], cred['pwd'])
startTime = datetime.datetime.now()

def map(f, R):
    return [t for (k,v) in R for t in f(k,v)]
    
def reduce(f, R):
    keys = {k for (k,v) in R}
    return [f(k1, [v for (k2,v) in R if k1 == k2]) for k1 in keys]

def reduce_func(k, vs):
	business_list = []
	failed = False
	for v in vs:
		if v[0] != 'Business Name' and v[1] == 'HE_Fail':
			failed = True
	return {k.replace('.', ''):failed}

restaurants = repo['ekwivagg_yuzhou7.restaurant'].find()
inspections = repo['ekwivagg_yuzhou7.inspection'].find()

restaurant_names = [(restaurant['businessname'], ('Business Name', restaurant['businessname'])) for restaurant in restaurants]
inspection_names = [(inspection['businessname'], ('Result', inspection['result'])) for inspection in inspections]

X = map(lambda k,v: [(k, ('Business Name', v))], restaurant_names) + map(lambda k,v: [(k, ('Results', v))], inspection_names)

Y = reduce(reduce_func, X)

repo.dropPermanent("inspection_failures")
repo.createPermanent("inspection_failures")
repo['ekwivagg_yuzhou7.inspection_failures'].insert_many(Y)

endTime = datetime.datetime.now()

doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ekwivagg_yuzhou7/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/ekwivagg_yuzhou7/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:inspection', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})


restaurant_dat = doc.entity('dat:restaurant', {prov.model.PROV_LABEL:'Restaurants', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
restaurant_retrieval = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})
doc.wasAssociatedWith(restaurant_retrieval, this_script)
doc.used(restaurant_retrieval, restaurant_dat, startTime)

inspection_dat = doc.entity('dat:hotline', {prov.model.PROV_LABEL:'inspection', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
inspection_retrieval = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})
doc.wasAssociatedWith(inspection_retrieval, this_script)
doc.used(inspection_retrieval, inspection_dat, startTime)


inspection_calc = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
doc.wasAssociatedWith(inspection_calc, this_script)
doc.used(inspection_calc, restaurant_dat, startTime)
doc.used(inspection_calc, inspection_dat, startTime)

inspection = doc.entity('dat:inspection', {prov.model.PROV_LABEL:'inspection', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
doc.wasAttributedTo(inspection, this_script)
doc.wasGeneratedBy(inspection, inspection_calc, endTime)
doc.wasDerivedFrom(inspection, restaurant_dat, inspection_calc, inspection_calc, inspection_calc)

repo.record(doc.serialize())
content = json.dumps(json.loads(doc.serialize()), indent=4)
f = open('plan.json', 'w')
f.write(content)
repo.logout()