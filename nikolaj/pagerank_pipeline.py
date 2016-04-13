import subprocess
import pymongo
import storeT
import storeBus
import combine_t_bus
import geoagg
import pagerank
import combineforviz
import export_pagerank
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

def drop_all_collections(repo):
    repo.dropPerm('nikolaj.contribs')
    repo.dropPerm('nikolaj.neighs')
    repo.dropPerm('nikolaj.ranks')
    repo.dropPerm('nikolaj.raw_bus_stops')
    repo.dropPerm('nikolaj.raw_stops')
    repo.dropPerm('nikolaj.raw_t_stops')
    repo.dropPerm('nikolaj.stops_with_neighs')
    repo.dropPerm('nikolaj.pagerank')
    repo.dropPerm('nikolaj.pagerank_result')
    repo.dropPerm('nikolaj.params')

def combine_and_export():
    combineforviz.run()
    export_pagerank.run()
    
def run_job_with_params(repo, job_params):
    drop_all_collections(repo)
    store_json(repo, 'nikolaj.params', job_params)

    storeT.run()
    storeBus.run()
    combine_t_bus.run()
    geoagg.run()
    pagerank.run()

t_only_params = [
    { "id" : "geoagg", "maxDistance" : 0, "routeUnion" : [ "$routes", "$geo_neigh_routes" ], "neighUnion" : [ "$neighs", "$geo_neighs" ] },
    { "id" : "combine_t_bus", "cols_to_combine" : [ "nikolaj.raw_t_stops" ] },
    { "id" : "pagerank_params", "output_col_name" : "nikolaj.pagerank_result_t_only" }
]

t_500walk_params = [
    { "id" : "geoagg", "maxDistance" : 500, "routeUnion" : [ "$routes", "$geo_neigh_routes" ], "neighUnion" : [ "$neighs", "$geo_neighs" ] },
    { "id" : "combine_t_bus", "cols_to_combine" : [ "nikolaj.raw_t_stops" ] },
    { "id" : "pagerank_params", "output_col_name" : "nikolaj.pagerank_result_t_500walk" }
]

job_param_queue = [t_only_params, t_500walk_params]
repo = get_auth_repo('nikolaj', 'nikolaj')
for job_param in job_param_queue:
    run_job_with_params(repo, job_param)
combine_and_export()
