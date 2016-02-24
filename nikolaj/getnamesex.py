import zipfile
import urllib.request
import csv
import codecs
import io
import json
import pymongo

def parse_line(line):
    no_new_line = line[:-1]
    name, gender, _ = no_new_line.split(',')
    return (name, {'name': name, 'gender': gender})

# returns a generator of each name gender mapping
# found in the zip archive
# the zip archive contains a file for each year,
# so duplicate name entries might be present
def get_items(url):
    raw_zf, headers = urllib.request.urlretrieve(url)
    with zipfile.ZipFile(raw_zf) as zf:
        for year_file in zf.namelist():
            if year_file[-3:] == 'pdf':
                continue
            with zf.open(year_file) as namegender:
                reader = io.TextIOWrapper(namegender, newline='')
                for line in reader:
                    yield parse_line(line)

def remove_dups(tups):
    return list(dict(tups).values())

# this function does database authentication for us and returns
# a valid repo object. note that without the workaround we can't
# call any methods added to the MongoClient object inside pymongo_dm
# since the methods make calls to customElevatedCommand which is scoped
# to only get_auth_repo unless we manually add in to the global environment
def get_auth_repo(uname, pwd):
    # below is a workaround for scoping issues of customElevatedCommand
    local_env = {}
    exec(open('../pymongo_dm.py').read(), {}, local_env)
    customElevatedCommand = local_env['customElevatedCommand']
    pathToConfig = local_env['pathToConfig']
    global customElevatedCommand, pathToConfig
    # end workaround
    client = pymongo.MongoClient()
    repo = client.repo
    repo.authenticate(uname, pwd)
    return repo

repo = get_auth_repo('nikolaj', 'nikolaj', globals())
print(globals())
repo.dropPermanent('name_gender_lookup')
repo.createPermanent('name_gender_lookup')
url = 'https://www.ssa.gov/oact/babynames/names.zip'
gender_lookup = remove_dups(get_items(url))
repo['nikolaj.name_gender_lookup'].insert_many(gender_lookup)
