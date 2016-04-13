import json
import pymongo
import copy
import ast
import urllib.request
import prov.model
import uuid
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

def run():
    repo = get_auth_repo('nikolaj', 'nikolaj')
    json_stations = get_stations()
    drop_all_collections(repo)
    store_stations(repo, json_stations)

def to_prov(startTime, endTime):
    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/nikolaj/') # The scripts in <folder>/<filename> format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/nikolaj/') # The data sets in <user>/<collection> format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    doc.add_namespace('mbt', 'https://github.com/mbtaviz/mbtaviz.github.io/tree/master/data')
    
    this_script = doc.agent('alg:storeT', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    # station_network
    station_network_resource = doc.entity('mbt:station-network', {'prov:label':'MBTA Station Network', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    get_station_network = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'Retrieve MBTA Station Network', prov.model.PROV_TYPE:'ont:Retrieval'})

    doc.wasAssociatedWith(get_station_network, this_script)
    doc.used(get_station_network, station_network_resource, startTime)

    # spider for visualization
    spider_resource = doc.entity('mbt:spider', {'prov:label':'Graphical representation of MBTA network', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    get_spider = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})

    doc.wasAssociatedWith(get_spider, this_script)
    doc.used(get_spider, spider_resource, startTime)

    station_network = doc.entity('dat:raw_t_stops', {prov.model.PROV_LABEL:'Derived T Stations', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(station_network, this_script)
    doc.wasGeneratedBy(station_network, get_station_network, endTime)
    doc.wasDerivedFrom(station_network, station_network_resource, get_station_network, get_station_network, get_station_network)
    doc.wasGeneratedBy(station_network, get_spider, endTime)
    doc.wasDerivedFrom(station_network, spider_resource, get_spider, get_spider, get_spider)
    return doc

if __name__ == "__main__":
    print(json.dumps(json.loads(to_prov(None, None).serialize()), indent=4))
