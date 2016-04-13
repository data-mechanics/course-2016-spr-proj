from os import listdir
from os.path import isfile, join
from bs4 import BeautifulSoup
import urllib.request
import pymongo
exec(open('../pymongo_dm.py').read())

def get_auth_repo(uname, pwd):
    client = pymongo.MongoClient()
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
            print(html)
            all_stops.extend(add_neighbors(parse_html(html, route)))
    return all_stops

def add_coords(stops, lookup):
    for stop in stops:
        stop_id = stop['id']
        lat, lng = lookup[stop_id]
        stop['coords'] = {'type': 'Point', 'coordinates': [float(lng), float(lat)]}
    return stops

repo = get_auth_repo('nikolaj', 'nikolaj')
lookup = coords_by_id(read_raw_stops('http://datamechanics.io/data/nikolaj/stops.txt'))
stops = read_stops_from_file('http://datamechanics.io/data/nikolaj/')
json_stops = add_coords(stops, lookup)
store_stations(repo, json_stops)
