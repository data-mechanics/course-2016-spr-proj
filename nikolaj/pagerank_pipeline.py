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
    repo.dropPerm('nikolaj.pagerank')
    repo.dropPerm('nikolaj.pagerank_result')
    repo.dropPerm('nikolaj.params')

def run_job_with_params(repo, job_params, doc):
    drop_derived_collections(repo)
    store_json(repo, 'nikolaj.params', job_params)
    startTime, _, endTime = geoagg.run()
    doc.update(geoagg.to_prov(startTime, endTime, job_params[0]))
    startTime, _, endTime = pagerank.run()
    doc.update(pagerank.to_prov(startTime, endTime, job_params[1]))

if __name__ == "__main__":
    repo = get_auth_repo('nikolaj', 'nikolaj')
    doc = prov.model.ProvDocument()
    
    startTime, _, endTime = storeT.run()
    doc.update(storeT.to_prov(startTime, endTime))
    startTime, _, endTime = storeBus.run()
    doc.update(storeBus.to_prov(startTime, endTime))
    
    t_only_params = [
        { "id" : "geoagg_params", "maxDistance" : 0, "output_col_name": "nikolaj.stops_with_neighs_t_only", "input_cols": [ "nikolaj.raw_t_stops" ], "routeUnion" : [ "$routes", "$geo_neigh_routes" ], "neighUnion" : [ "$neighs", "$geo_neighs" ] },
        { "id" : "pagerank_params", "input_col_name": "nikolaj.stops_with_neighs_t_only", "output_col_name" : "nikolaj.pagerank_result_t_only" }
    ]

    t_500walk_params = [
        { "id" : "geoagg_params", "maxDistance" : 500, "output_col_name": "nikolaj.stops_with_neighs_t_500walk", "input_cols": [ "nikolaj.raw_t_stops" ], "routeUnion" : [ "$routes", "$geo_neigh_routes" ], "neighUnion" : [ "$neighs", "$geo_neighs" ] },
        { "id" : "pagerank_params", "input_col_name": "nikolaj.stops_with_neighs_t_500walk", "output_col_name" : "nikolaj.pagerank_result_t_500walk" }
    ]

    t_500walk_bus_params = [
        { "id" : "geoagg_params", "maxDistance" : 500, "output_col_name": "nikolaj.stops_with_neighs_t_500walk_bus", "input_cols": [ "nikolaj.raw_t_stops", "nikolaj.raw_bus_stops" ], "routeUnion" : [ "$routes", "$geo_neigh_routes" ], "neighUnion" : [ "$neighs", "$geo_neighs" ] },
        { "id" : "pagerank_params", "input_col_name": "nikolaj.stops_with_neighs_t_500walk_bus", "output_col_name" : "nikolaj.pagerank_result_t_500walk_bus" }
    ]
    job_param_queue = [t_only_params, t_500walk_params, t_500walk_bus_params]
    
    for job_param in job_param_queue:
        run_job_with_params(repo, job_param, doc)
    
    startTime, _, endTime = vizdatautil.run()
    doc.update(vizdatautil.to_prov(startTime, endTime))

    repo.record(doc.serialize()) # Record the provenance document.
    print(doc.get_provn())
    