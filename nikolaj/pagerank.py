import subprocess
import prov.model
import json
import uuid

def run():
    returncode = subprocess.call(["mongo repo -u nikolaj -p nikolaj < pagerank.js"], shell=True)
    return returncode

def to_prov(startTime, endTime):
    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/nikolaj/') # The scripts in <folder>/<filename> format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/nikolaj/') # The data sets in <user>/<collection> format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    
    this_script = doc.agent('alg:pagerank', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
    stops_with_neighs = doc.entity('dat:stops_with_neighs', {prov.model.PROV_LABEL:'Stops aggregated by location', prov.model.PROV_TYPE:'ont:DataSet'})
    pagerank = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'Compute PageRank for Stops', prov.model.PROV_TYPE:'Computation'})

    doc.wasAssociatedWith(pagerank, this_script)
    doc.usage(pagerank, stops_with_neighs, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
    
    pagerank_results = doc.entity('dat:pagerank_results', {prov.model.PROV_LABEL:'Stops and their computed PageRank', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(pagerank_results, this_script)
    doc.wasGeneratedBy(pagerank_results, pagerank, endTime)
    doc.wasDerivedFrom(pagerank_results, stops_with_neighs, pagerank, pagerank, pagerank)
    return doc

if __name__ == "__main__":
    print(json.dumps(json.loads(to_prov(None, None).serialize()), indent=4))

