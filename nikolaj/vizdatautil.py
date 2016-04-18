import subprocess
import prov.model
import json
import uuid
from utils import timestamped

def export(collection, file_name):
    export_cmd = "mongoexport -u nikolaj -p nikolaj --db repo --collection nikolaj.{0} --out {1}".format(collection, file_name)
    returncode = subprocess.call([export_cmd], shell=True)
    with open(file_name) as f:
        raw = f.read()
        raw = '[' + raw.replace('\n', ',\n')[:-2] + ']'
    with open(file_name, 'w') as f:
        f.write(raw)

@timestamped    
def run():
    subprocess.call(["mongo repo -u nikolaj -p nikolaj < vizdatautil.js"], shell=True)
    export('pagerank_result', 'ranks.json')
    export('pagerank_result_t_500walk_bus', 'ranks-with-bus.json')
    export('dist_routes', 'routes.json')

def to_prov(startTime, endTime):
    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/nikolaj/') # The scripts in <folder>/<filename> format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/nikolaj/') # The data sets in <user>/<collection> format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    
    this_script = doc.agent('alg:vizdatautil', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
    t_only = doc.entity('dat:pagerank_result_t_only', {prov.model.PROV_LABEL:'Stops and their computed PageRank', prov.model.PROV_TYPE:'ont:DataSet'})
    t_walk = doc.entity('dat:pagerank_result_t_500walk', {prov.model.PROV_LABEL:'Stops and their computed PageRank', prov.model.PROV_TYPE:'ont:DataSet'})
    t_walk_bus = doc.entity('dat:pagerank_result_t_500walk_bus', {prov.model.PROV_LABEL:'Stops and their computed PageRank', prov.model.PROV_TYPE:'ont:DataSet'})
    
    combine_ranks = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'Combine different pageranks into single collection', prov.model.PROV_TYPE:'Computation'})

    doc.wasAssociatedWith(combine_ranks, this_script)
    doc.usage(combine_ranks, t_only, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
    doc.usage(combine_ranks, t_walk, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
    doc.usage(combine_ranks, t_walk_bus, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
    
    pagerank_results = doc.entity('dat:pagerank_result', {prov.model.PROV_LABEL:'All pageranks in single collection', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(pagerank_results, this_script)
    doc.wasGeneratedBy(pagerank_results, combine_ranks, endTime)
    doc.wasDerivedFrom(pagerank_results, t_only, combine_ranks, combine_ranks, combine_ranks)
    doc.wasDerivedFrom(pagerank_results, t_walk, combine_ranks, combine_ranks, combine_ranks)
    doc.wasDerivedFrom(pagerank_results, t_walk_bus, combine_ranks, combine_ranks, combine_ranks)
    
    # do routes here
    get_routes = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'Extract all distinct routes', prov.model.PROV_TYPE:'Computation'})
    doc.wasAssociatedWith(get_routes, this_script)
    doc.usage(get_routes, pagerank_results, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
    
    routes = doc.entity('dat:dist_routes', {prov.model.PROV_LABEL:'All distinct routes', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(routes, this_script)
    doc.wasGeneratedBy(routes, get_routes, endTime)
    doc.wasDerivedFrom(routes, pagerank_results, get_routes, get_routes, get_routes)
   
    return doc

if __name__ == "__main__":
    print(json.dumps(json.loads(to_prov(None, None).serialize()), indent=4))
