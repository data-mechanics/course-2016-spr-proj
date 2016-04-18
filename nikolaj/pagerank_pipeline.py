import subprocess
import pymongo
import storeT
import storeBus
import geoagg
import pagerank
import vizdatautil
import prov.model
exec(open('../pymongo_dm.py').read())

def get_auth_repo(uname, pwd):
    client = pymongo.MongoClient()
    repo = client.repo
    repo.authenticate(uname, pwd)
    return repo

def store_json(repo, target_col_name, raw_json):
    repo.dropPerm(target_col_name)
    repo.createPerm(target_col_name)
    repo[target_col_name].insert_many(raw_json)

def drop_derived_collections(repo):
    repo.dropPerm('nikolaj.contribs')
    repo.dropPerm('nikolaj.neighs')
    repo.dropPerm('nikolaj.ranks')
    repo.dropPerm('nikolaj.raw_stops')
    repo.dropPerm('nikolaj.stops_with_neighs')
    repo.dropPerm('nikolaj.pagerank')
    repo.dropPerm('nikolaj.pagerank_result')
    repo.dropPerm('nikolaj.params')

def export_for_viz():
    vizdatautil.run()

def to_plan(repo):
    doc = prov.model.ProvDocument()
    doc.update(storeT.to_prov(None, None))
    doc.update(storeBus.to_prov(None, None))
    doc.update(geoagg.to_prov(None, None))
    doc.update(pagerank.to_prov(None, None))
    
    repo.record(doc.serialize()) # Record the provenance document.
    with open('plan.json','w') as plan:
        plan.write(json.dumps(json.loads(doc.serialize()), indent=4))
    print(doc.get_provn())

def load_initial_data(repo):
    storeT.run()
    storeBus.run()
    
def run_job_with_params(repo, job_params):
    drop_derived_collections(repo)
    store_json(repo, 'nikolaj.params', job_params)
    geoagg.run()
    pagerank.run()

if __name__ == "__main__":
    repo = get_auth_repo('nikolaj', 'nikolaj')
    load_initial_data(repo)
    
    t_only_params = [
        { "id" : "geoagg_params", "maxDistance" : 0, "input_cols": [ "nikolaj.raw_t_stops" ], "routeUnion" : [ "$routes", "$geo_neigh_routes" ], "neighUnion" : [ "$neighs", "$geo_neighs" ] },
        { "id" : "pagerank_params", "output_col_name" : "nikolaj.pagerank_result_t_only" }
    ]

    t_500walk_params = [
        { "id" : "geoagg_params", "maxDistance" : 500, "input_cols": [ "nikolaj.raw_t_stops" ], "routeUnion" : [ "$routes", "$geo_neigh_routes" ], "neighUnion" : [ "$neighs", "$geo_neighs" ] },
        { "id" : "pagerank_params", "output_col_name" : "nikolaj.pagerank_result_t_500walk" }
    ]

    t_500walk_bus_params = [
        { "id" : "geoagg_params", "maxDistance" : 500, "input_cols": [ "nikolaj.raw_t_stops", "nikolaj.raw_bus_stops" ], "routeUnion" : [ "$routes", "$geo_neigh_routes" ], "neighUnion" : [ "$neighs", "$geo_neighs" ] },
        { "id" : "pagerank_params", "output_col_name" : "nikolaj.pagerank_result_t_500walk_bus" }
    ]
    job_param_queue = [t_only_params, t_500walk_params, t_500walk_bus_params]
    
    for job_param in job_param_queue:
        run_job_with_params(repo, job_param)
    
    export_for_viz()
