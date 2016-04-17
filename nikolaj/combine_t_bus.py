import subprocess
import prov.model
import json
import uuid

def run():
    returncode = subprocess.call(["mongo repo -u nikolaj -p nikolaj < combine_t_bus.js"], shell=True)
    return returncode

def to_prov(startTime, endTime):
    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/nikolaj/') # The scripts in <folder>/<filename> format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/nikolaj/') # The data sets in <user>/<collection> format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    
    this_script = doc.agent('alg:combine_t_bus', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
    raw_t_stops = doc.entity('dat:raw_t_stops', {prov.model.PROV_LABEL:'T Stops', prov.model.PROV_TYPE:'ont:DataSet'})

    raw_bus_stops = doc.entity('dat:raw_bus_stops', {prov.model.PROV_LABEL:'Bus Stops', prov.model.PROV_TYPE:'ont:DataSet'}) 
    combine_t_bus_data = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'Combine T and Bus Data', prov.model.PROV_TYPE:'Computation'})

    doc.wasAssociatedWith(combine_t_bus_data, this_script)
    doc.usage(combine_t_bus_data, raw_bus_stops, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
    doc.usage(combine_t_bus_data, raw_t_stops, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

    raw_stops = doc.entity('dat:raw_stops', {prov.model.PROV_LABEL:'Combined T and Bus Data', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(raw_stops, this_script)
    doc.wasGeneratedBy(raw_stops, combine_t_bus_data, endTime)
    doc.wasDerivedFrom(raw_stops, raw_bus_stops, combine_t_bus_data, combine_t_bus_data, combine_t_bus_data)
    doc.wasDerivedFrom(raw_stops, raw_t_stops, combine_t_bus_data, combine_t_bus_data, combine_t_bus_data)
    return doc

if __name__ == "__main__":
    print(json.dumps(json.loads(to_prov(None, None).serialize()), indent=4))
