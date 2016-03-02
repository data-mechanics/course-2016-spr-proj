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
	rodent = False
	for v in vs:
		if v[0] == 'Business Name':
			business_list.append(v[1])
		else:
			if v[1]:
				rodent = True
	return {(k[0]+'_'+k[1]).replace('.', ','):{'businesses': business_list, 'rodent_problem':rodent}}

restaurants = repo['ekwivagg_yuzhou7.restaurant'].find()
rodents = repo['ekwivagg_yuzhou7.hotline'].find()

restaurant_locations = [((restaurant['location']['latitude'][:6], restaurant['location']['longitude'][:7]), restaurant['businessname']) for restaurant in restaurants]
rodent_locations = [((rodent['geocoded_location']['latitude'][:6], rodent['geocoded_location']['longitude'][:7]), True) for rodent in rodents]

X = map(lambda k,v: [(k, ('Business Name', v))], restaurant_locations) + map(lambda k,v: [(k, ('Rodent Problem', v))], rodent_locations)

Y = reduce(reduce_func , X)

repo.dropPermanent("rodent_problem")
repo.createPermanent("rodent_problem")
repo['ekwivagg_yuzhou7.rodent_problem'].insert_many(Y)

endTime = datetime.datetime.now()

doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ekwivagg_yuzhou7/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/ekwivagg_yuzhou7/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:rodent_problem', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})


restaurant_dat = doc.entity('dat:restaurant', {prov.model.PROV_LABEL:'Restaurants', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
restaurant_retrieval = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})
doc.wasAssociatedWith(restaurant_retrieval, this_script)
doc.used(restaurant_retrieval, restaurant_dat, startTime)

rodent_dat = doc.entity('dat:hotline', {prov.model.PROV_LABEL:'Rodent', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
rodent_retrieval = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})
doc.wasAssociatedWith(rodent_retrieval, this_script)
doc.used(rodent_retrieval, rodent_dat, startTime)


rodent_problem_calc = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
doc.wasAssociatedWith(rodent_problem_calc, this_script)
doc.used(rodent_problem_calc, restaurant_dat, startTime)
doc.used(rodent_problem_calc, rodent_dat, startTime)

rodent_problem = doc.entity('dat:rodent_problem', {prov.model.PROV_LABEL:'Rodent Problem', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
doc.wasAttributedTo(rodent_problem, this_script)
doc.wasGeneratedBy(rodent_problem, rodent_problem_calc, endTime)
doc.wasDerivedFrom(rodent_problem, restaurant_dat, rodent_problem_calc, rodent_problem_calc, rodent_problem_calc)

repo.record(doc.serialize())
content = json.dumps(json.loads(doc.serialize()), indent=4)
f = open('plan.json', 'w')
f.write(content)
repo.logout()
