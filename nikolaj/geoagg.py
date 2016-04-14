import subprocess
import prov.model
import json
import uuid

def run():
    returncode = subprocess.call(["mongo repo -u nikolaj -p nikolaj < geoagg.js"], shell=True)
    return returncode

def to_prov(startTime, endTime):
    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/nikolaj/') # The scripts in <folder>/<filename> format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/nikolaj/') # The data sets in <user>/<collection> format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    
    this_script = doc.agent('alg:geoagg', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
    raw_stops = doc.entity('dat:raw_stops', {prov.model.PROV_LABEL:'T and Bus Stops', prov.model.PROV_TYPE:'ont:DataSet'})
    geo_agg = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'Aggregate Stops Based on Geolocation', prov.model.PROV_TYPE:'Computation'})

    doc.wasAssociatedWith(geo_agg, this_script)
    doc.usage(geo_agg, raw_stops, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
    
    stops_with_neighs = doc.entity('dat:stops_with_neighs', {prov.model.PROV_LABEL:'Stops aggregated by location', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(stops_with_neighs, this_script)
    doc.wasGeneratedBy(stops_with_neighs, geo_agg, endTime)
    doc.wasDerivedFrom(stops_with_neighs, raw_stops, geo_agg, geo_agg, geo_agg)
    return doc

if __name__ == "__main__":
    print(json.dumps(json.loads(to_prov(None, None).serialize()), indent=4))

