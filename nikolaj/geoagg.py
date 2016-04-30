import subprocess
import prov.model
import json
import uuid
from utils import timestamped

@timestamped
def run():
    returncode = subprocess.call(["mongo repo -u nikolaj -p nikolaj < geoagg.js"], shell=True)
    return returncode

def to_prov(startTime, endTime, params):
    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/nikolaj/') # The scripts in <folder>/<filename> format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/nikolaj/') # The data sets in <user>/<collection> format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    
    this_script = doc.agent('alg:geoagg', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
    
    output_col_name = params['output_col_name'].split('.')[1]

    stops_with_neighs = doc.entity('dat:' + output_col_name, {prov.model.PROV_LABEL:'Stops aggregated by location', prov.model.PROV_TYPE:'ont:DataSet'})
    
    input_stop_cols = params['input_cols']
    geo_agg = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'Aggregate Stops Based on Geolocation', prov.model.PROV_TYPE:'Computation'})
    
    for stop_col_name in input_stop_cols:
        input_stop = doc.entity('dat:' + stop_col_name.split('.')[1], {prov.model.PROV_LABEL:'Stops', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.usage(geo_agg, input_stop, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
    
        doc.wasAttributedTo(stops_with_neighs, this_script)
        doc.wasGeneratedBy(stops_with_neighs, geo_agg, endTime)
        doc.wasDerivedFrom(stops_with_neighs, input_stop, geo_agg, geo_agg, geo_agg)
    
    doc.wasAssociatedWith(geo_agg, this_script)
    return doc

if __name__ == "__main__":
    mock_params = { "id" : "geoagg_params", "maxDistance" : 0, "input_cols": [ "nikolaj.raw_t_stops", "nikolaj.raw_bus_stops" ], "routeUnion" : [ "$routes", "$geo_neigh_routes" ], "neighUnion" : [ "$neighs", "$geo_neighs" ] }
        
    print(json.dumps(json.loads(to_prov(None, None, mock_params).serialize()), indent=4))

