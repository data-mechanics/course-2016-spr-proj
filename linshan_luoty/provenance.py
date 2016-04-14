import prov
import json

def init():
	f = open('plan.json', 'r')
	doc = prov.read(f)
	f.close()
	doc.add_namespace('alg', 'https://data-mechanics.s3.amazonaws.com/linshan_luoty/algorithm/') # The scripts in <folder>/<filename> format.
	doc.add_namespace('dat', 'https://data-mechanics.s3.amazonaws.com/linshan_luoty/data/') # The data sets in <user>/<collection> format.
	doc.add_namespace('ont', 'https://data-mechanics.s3.amazonaws.com/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
	doc.add_namespace('log', 'https://data-mechanics.s3.amazonaws.com/log#') # The event log.
	doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
	return doc

def update(doc):
	f = open('plan.json', 'w')
	f.write(json.dumps(json.loads(doc.serialize()), indent=4))
	f.close()