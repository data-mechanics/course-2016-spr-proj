from os import listdir
from os.path import isfile, join
from bs4 import BeautifulSoup
import urllib.request
import prov.model
import uuid
import dml
from utils import timestamped

def get_auth_repo(uname, pwd):
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate(uname, pwd)
    return repo

def store_json(repo, taget_col_name, raw_json):
    repo.createPerm(taget_col_name)
    repo[taget_col_name].insert_many(raw_json)

def store_stations(repo, json_stations):
    store_json(repo, 'nikolaj.raw_bus_stops', json_stations)

def drop_all_collections(repo):
    pass

def read_raw_stops(url):
    with urllib.request.urlopen(url) as st_f:
        raw = st_f.read().decode("utf-8").split('\n')
        headers = [s.replace('"', '') for s in raw[0].split(',')]
        zipped = [dict(zip(headers, [s.replace('"', '') for s in row.split(',')])) for row in raw[1:]]
        return zipped

def isnum(s):
    try:
        return int(s)
    except ValueError:
        return None

def coords_by_id(zipped):
    z = [(s['stop_id'], (s['stop_lat'], s['stop_lon'])) for s in zipped if isnum(s['stop_id'])]
    return dict(z)

def parse_html(html, route):
    soup = BeautifulSoup(html, 'html.parser')
    els = soup.findAll(['li', 'h2'])
    headers_seen = 0
    stops = []
    for el in els:
        if el.name == 'h2':
            headers_seen += 1
        elif headers_seen == 1:
            stop_id = str(int(el.a['href'].split('/')[-1].split('_')[0]))
            stop_name = el.getText().strip()
            stop = {"id": stop_id, "name": stop_name, "neighs": [], "routes": [route]}
            stops.append(stop)
        elif headers_seen == 2:
            break
    return stops

def add_neighbors(stops):
    prev_station = None
    for st in stops:
        if prev_station:
            st['neighs'].append(prev_station['id'])
            prev_station['neighs'].append(st['id'])
        prev_station = st
    return stops

def read_stops_from_file(baseurl):
    all_stops = []
    onlyfiles = ['1.html', '101.html', '105.html', '116.html', '16.html', '23.html', '39.html', '57.html', '66.html', '70.html', '83.html', '86.html', '89.html']
    for fn in onlyfiles:
        with urllib.request.urlopen(baseurl + fn) as loaded:
            route = fn.split('.')[0]
            html = loaded.read().decode("utf-8")
            all_stops.extend(add_neighbors(parse_html(html, route)))
    return all_stops

def add_coords(stops, lookup):
    for stop in stops:
        stop_id = stop['id']
        lat, lng = lookup[stop_id]
        stop['coords'] = {'type': 'Point', 'coordinates': [float(lng), float(lat)]}
    return stops

@timestamped
def run():
    repo = get_auth_repo('nikolaj', 'nikolaj')
    lookup = coords_by_id(read_raw_stops('http://datamechanics.io/data/nikolaj/stops.txt'))
    stops = read_stops_from_file('http://datamechanics.io/data/nikolaj/')
    json_stops = add_coords(stops, lookup)
    store_stations(repo, json_stops)

def to_prov(startTime, endTime):
    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/nikolaj/') # The scripts in <folder>/<filename> format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/nikolaj/') # The data sets in <user>/<collection> format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    doc.add_namespace('mbt', 'http://www.mbtainfo.com/')
    
    this_script = doc.agent('alg:storeBus', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    # stops
    stops = ['1', '101', '105', '116', '16', '23', '39', '57', '66', '70', '83', '86', '89']
    raw_bus_stops = doc.entity('dat:raw_bus_stops', {prov.model.PROV_LABEL:'Stops', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(raw_bus_stops, this_script)
    for stop in stops:
        stop_resource = doc.entity('mbt:' + stop , {'prov:label':'MBTA Bus Stop ' + stop, prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'html'})
        get_stop = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'MBTA Bus Stop ' + stop, prov.model.PROV_TYPE:'ont:Retrieval'})

        doc.wasAssociatedWith(get_stop, this_script)
        doc.used(get_stop, stop_resource, startTime)

        doc.wasGeneratedBy(raw_bus_stops, get_stop, endTime)
        doc.wasDerivedFrom(raw_bus_stops, stop_resource, get_stop, get_stop, get_stop)
    return doc

if __name__ == "__main__":
    print(json.dumps(json.loads(to_prov(None, None).serialize()), indent=4))
