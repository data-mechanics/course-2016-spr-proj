from urllib import request, parse
import json
import pymongo
import prov.model
import datetime
import uuid
import time

exec(open('../pymongo_dm.py').read())

# set up connection

client = pymongo.MongoClient()
repo = client.repo

# remember to remove this line later
repo.authenticate("jgyou", "jgyou")

# retrieve City of Boston data on needle program

startTime = datetime.datetime.now()

query = "Needle Pickup"

urlbdp = "https://data.cityofboston.gov/resource/wc8w-nujj.json?type=" + parse.quote_plus(query)
responsebdp = request.urlopen(urlbdp).read().decode("utf-8")
r1 = json.loads(responsebdp)
repo.dropPermanent("needle311")
repo.createPermanent("needle311")
#s = json.dumps(r, sort_keys=True, indent=2)
repo['jgyou.needle311'].insert_many(r1)

endTime = datetime.datetime.now()


# record provenance data

provdoc = prov.model.ProvDocument()
provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/jgyou/') # The scripts in <folder>/<filename> format.
provdoc.add_namespace('dat', 'http://datamechanics.io/data/jgyou/') # The data sets in <user>/<collection> format.
provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
provdoc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')		# city of boston data.


this_script = provdoc.agent('alg:retrievedata', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource = provdoc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
this_run = provdoc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?type=Needle Pickup'})
provdoc.wasAssociatedWith(this_run, this_script)
provdoc.used(this_run, resource, startTime)

needle311 = provdoc.entity('dat:needle311', {prov.model.PROV_LABEL:'Needle Program', prov.model.PROV_TYPE:'ont:DataSet'})
provdoc.wasAttributedTo(needle311, this_script)
provdoc.wasGeneratedBy(needle311, this_run, endTime)
provdoc.wasDerivedFrom(needle311, resource, this_run, this_run, this_run)

repo.record(provdoc.serialize()) # Record the provenance document.
print(provdoc.get_provn())
repo.logout()



