import subprocess
import pymongo
import storeT
import storeBus
import combine_t_bus
import geoagg
import pagerank
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
    repo.dropPerm('nikolaj.params')

def combine_and_export():
    pass
    # combineforviz = "mongo repo < combineforviz.js"
    # export_pagerank = "python3.4 export_pagerank.py"
    # subprocess.call([combineforviz], shell=True)
    # subprocess.call([export_pagerank], shell=True)

def run_job_with_params(repo, job_params):
    drop_all_collections(repo)
    store_json(repo, 'nikolaj.params', job_params)

    storeT.run()
    storeBus.run()
    combine_t_bus.run()
    geoagg.run()
    pagerank.run()

    # store_T = "python3.4 storeT.py"
    # store_bus = "python3.4 storeBus.py"

    # combine_T_bus = "mongo repo < combine_t_bus.js"
    # find_near = "mongo repo < geoagg.js"
    # pagerank = "mongo repo < pagerank.js"
    
    # all_cmds = [store_T, store_bus, combine_T_bus, find_near, pagerank]

    # for cmd in all_cmds:
    #     returncode = subprocess.call([cmd], shell=True)
    #     print(returncode)

t_only_params = [
    { "id" : "geoagg", "maxDistance" : 0, "routeUnion" : [ "$routes", "$geo_neigh_routes" ], "neighUnion" : [ "$neighs", "$geo_neighs" ] },
    { "id" : "combine_t_bus", "cols_to_combine" : [ "nikolaj.raw_t_stops" ] },
    { "id" : "pagerank_params", "output_col_name" : "nikolaj.pagerank_result_t_only" }
]

# t_500walk_params = [
#     { "id" : "geoagg", "maxDistance" : 500, "routeUnion" : [ "$routes", "$geo_neigh_routes" ], "neighUnion" : [ "$neighs", "$geo_neighs" ] },
#     { "id" : "combine_t_bus", "cols_to_combine" : [ "nikolaj.raw_t_stops" ] },
#     { "id" : "pagerank_params", "output_col_name" : "pagerank_result_t_500walk" }
# ]

job_param_queue = [t_only_params]

repo = get_auth_repo('nikolaj', 'nikolaj')
for job_param in job_param_queue:
    run_job_with_params(repo, job_param)
combine_and_export()
