import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid


# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('billguo', 'billguo')

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

url = 'https://data.cityofboston.gov/resource/54s2-yxpg.json?$limit=500'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("earn2013")
repo.createPermanent("earn2013")
repo['billguo.earn2013'].insert_many(r)

url = 'https://data.cityofboston.gov/resource/4swk-wcg8.json?$limit=500'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("earn2014")
repo.createPermanent("earn2014")
repo['billguo.earn2014'].insert_many(r)

url = 'https://data.cityofboston.gov/resource/effb-uspk.json?$limit=500'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("earn2012")
repo.createPermanent("earn2012")
repo['billguo.earn2012'].insert_many(r)

repo.createPermanent("earn2012to2014")

earn2013 = repo['billguo.earn2013']
earn2014 = repo['billguo.earn2014']
earn2012 = repo['billguo.earn2012']


R = dict()
R["name"] = "job&earn2014"
R["children"] = []
temp = ""
temp2 = ""
for data in earn2014.find():
	if data["department_name"] != temp: #find a new department
		temp = data["department_name"]
		D = dict()
		D["name"] = temp
		D["children"] = []
		for data2 in earn2014.find({"department_name":temp}):
			if data2["title"] != temp2: #find a new title
				temp2 = data2["title"]
				T = dict()
				T["name"] = temp2
				T["children"] = []	
				a = earn2014.find({"department_name":temp,"title": temp2}, {"name":True, "department_name":True, "title":True,"total_earnings":True, "_id":False})
				l = list(a)
				json.dumps(l)
				T["children"] = l
			else:
				continue
			D["children"].append(T)
	else:
		continue
	R["children"].append(D)


j = []
j.append(R)

repo.dropPermanent("test")
repo.createPermanent("test")
json.dumps(j, sort_keys=True, indent=2)
repo['billguo.test'].insert_many(j)


		

#this script basically search all the data in 2012 and than look for
#data with same name in 2013 and 2014 and put their earnings in 2013
#and 2014 as earn2013 and earn2014 in a new collection
count1 = 0
earntotal = []
for data in earn2012.find():
	te2013 = earn2013.find_one({"name":data["name"]}) #record the total_earnings in 2013
	if te2013 is None: #if person not found, skip this
		continue
	else:
		te2014 = earn2014.find_one({"name":data["name"]}) #similar to above
		if te2014 is None:
			continue
		else:
			data["earn2013"] = te2013["total_earnings"] #add the coloums
			data["earn2014"] = te2014["total_earnings"]
			earntotal.append(data)
			count1+=1
			#print("1")

repo.dropPermanent("earntotal")
repo.createPermanent("earntotal")
json.dumps("earntotal", sort_keys=True, indent=2)
repo['billguo.earntotal'].insert_many(earntotal)


#this script bascially through three collecions and find out the people who
#earn more and more each years
count2 = 0
rich = []
for data in earn2012.find():
	te2013 = earn2013.find_one({"name":data["name"]})
	if te2013 is None:
		continue
	else:
		te2014 = earn2014.find_one({"name":data["name"]})
		if te2014 is None:
			continue
		else:
			if (float(te2013["total_earnings"]) < float(te2014["total_earnings"])):
				rich.append(data)
				count2+=1
				#print("2")

repo.dropPermanent("rich")
repo.createPermanent("rich")
json.dumps("rich", sort_keys=True, indent=2)
repo['billguo.rich'].insert_many(rich)
result = count2 / count1 * 100.0
print("the percentage of people that have increasing earnings each year is " + str(result) + '%')

increasing = repo['billguo.rich']
S = dict()
S["name"] = "increasing_earning"
S["children"] = []
S1 = ""
S2 = ""
for data in increasing.find():
	if data["department"] != S1: #find a new department
		S1 = data["department"]
		D = dict()
		D["name"] = S1
		D["children"] = []
		for data2 in increasing.find({"department":S1}):
			if data2["title"] != S2: #find a new title
				S2 = data2["title"]
				T = dict()
				T["name"] = S2
				T["children"] = []	
				a = increasing.find({"department":S1,"title": S2}, {"name":True, "department":True, "tilte":True, "total_earnings":True, "_id":False})
				l = list(a)
				json.dumps(l)
				T["children"] = l
			else:
				continue
			D["children"].append(T)
	else:
		continue
	S["children"].append(D)


k = []
k.append(S)

repo.dropPermanent("increasing_earning")
repo.createPermanent("increasing_earning")
json.dumps(k, sort_keys=True, indent=2)
repo['billguo.increasing_earning'].insert_many(k)

endTime = datetime.datetime.now()


# Create the provenance document describing everything happening
# in this script. Each run of the script will generate a new
# document describing that invocation event. This information
# can then be used on subsequent runs to determine dependencies
# and "replay" everything. The old documents will also act as a
# log.
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/billguo/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/billguo/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:project1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource = doc.entity('bdp:effb-uspk', {'prov:label':'Employee Earnings Report 2012', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
resource2 = doc.entity('bdp:54s2-yxpg', {'prov:label':'Employee Earnings Report 2013', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
resource3 = doc.entity('bdp:qz7u-kb7x', {'prov:label':'Employee Earnings Report 2014', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

totalearn = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime)
rich = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime)
doc.wasAssociatedWith(totalearn, this_script)
doc.wasAssociatedWith(rich, this_script)

rich = doc.entity('dat:rich', {prov.model.PROV_LABEL:'rich', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(rich, this_script)
doc.wasGeneratedBy(rich, rich, endTime)
doc.wasDerivedFrom(rich, resource, rich, rich, rich)

totalearn = doc.entity('dat:totalearn', {prov.model.PROV_LABEL:'totalearn', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(totalearn, this_script)
doc.wasGeneratedBy(totalearn, totalearn, endTime)
doc.wasDerivedFrom(totalearn, resource, totalearn, totalearn, totalearn)

repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()


## eof
