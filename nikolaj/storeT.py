import json
import pymongo
import copy
import ast
import urllib.request
exec(open('../pymongo_dm.py').read())

def get_auth_repo(uname, pwd):
    client = pymongo.MongoClient()
    repo = client.repo
    repo.authenticate(uname, pwd)
    return repo

def drop_all_collections(repo):
    pass

def store_json(repo, taget_col_name, raw_json):
    repo.createPerm(taget_col_name)
    repo[taget_col_name].insert_many(raw_json)

def store_stations(repo, json_stations):
    store_json(repo, 'nikolaj.raw_t_stops', json_stations)

def read_graph(url):
    with urllib.request.urlopen(url) as loaded:
        response = loaded.read().decode("utf-8")
        return ast.literal_eval(response)
        
def read_raw_stops(url):
    with urllib.request.urlopen(url) as st_f:
        raw = st_f.read().decode("utf-8").split('\n')
        headers = [s.replace('"', '') for s in raw[0].split(',')]
        zipped = [dict(zip(headers, [s.replace('"', '') for s in row.split(',')])) for row in raw[1:]]
        return zipped

def coords_by_id(zipped):
    return dict([(s['stop_id'], (s['stop_lat'], s['stop_lon'])) for s in zipped])

def get_stations():
    coords_lookup = coords_by_id(read_raw_stops('http://datamechanics.io/data/nikolaj/stops.txt'))
    graph = read_graph('http://datamechanics.io/data/nikolaj/graph.json')
    stations, links = graph['nodes'], graph['links']
    for st in stations:
        coords = coords_lookup[st['id']]
        st['lat'], st['lng'] = coords[0], coords[1]
        st['neighs'] = set()
        st['routes'] = set()
    for link in links:
        target_node = stations[link['target']]
        source_node = stations[link['source']]
        target_node['neighs'].add(source_node['id'])
        source_node['neighs'].add(target_node['id'])
        target_node['routes'].add(link['line'])
        source_node['routes'].add(link['line'])
    for st in stations:
        st['neighs'] = list(st['neighs'])
        st['routes'] = list(st['routes'])
        st['coords'] = {'type': 'Point', 'coordinates': [float(st['lng']), float(st['lat'])]}
    # TODO: might want to postprocess for green line
    return stations

# g = read_graph('http://datamechanics.io/data/nikolaj/graph.json')
# print(g)
repo = get_auth_repo('nikolaj', 'nikolaj')
json_stations = get_stations()
drop_all_collections(repo)
store_stations(repo, json_stations)
