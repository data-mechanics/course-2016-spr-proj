import zipfile
import urllib.request
import io
import json
import pymongo

'''
Any object created via exec(open(path_to_pymongo_dm).read())
(including customElevatedCommand) only exists within the scope
of the function which made the exec call. Therefore, if we 
instantiate a repo object inside a function and return it, all 
custom class methods (createTemp, etc.) will fail since the 
reference to customElevatedCommand will be lost.

The below method manually elevates customElevatedCommand and
pathToConfig to the global scope.
THIS IS ONLY A WORKAROUND. Once pymongo_dm becomes a library
the import mechanism will resolve the issue.
'''
def bandaid_exec(path_to_pymongo_dm='../pymongo_dm.py'):
    local_env = {}
    exec(open(path_to_pymongo_dm).read(), globals(), local_env)
    customElevatedCommand = local_env['customElevatedCommand']
    pathToConfig = local_env['pathToConfig']
    global customElevatedCommand, pathToConfig

def get_auth_repo(uname, pwd):
    bandaid_exec()
    client = pymongo.MongoClient()
    repo = client.repo
    repo.authenticate(uname, pwd)
    return repo

def remove_dups(tups):
    return list(dict(tups).values())

def store_name_gender_lookup(repo):
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
                if year_file[-3:] == 'pdf': # skip pdf readme
                    continue
                with zf.open(year_file) as namegender:
                    reader = io.TextIOWrapper(namegender, newline='')
                    for line in reader:
                        yield parse_line(line)

    url = 'https://www.ssa.gov/oact/babynames/names.zip'
    gender_lookup = remove_dups(get_items(url))
    repo['nikolaj.name_gender_lookup'].insert_many(gender_lookup)

def retrieve_and_store_json(url, target, repo):
    response = urllib.request.urlopen(url).read().decode("utf-8")
    r = json.loads(response)
    repo.createPermanent(target)
    repo['nikolaj.' + target].insert_many(r)
    
def store_salary_data(repo):
    url = 'https://data.cityofboston.gov/resource/ntv7-hwjm.json'
    retrieve_and_store_json(url, 'earnings_2014', repo)

def drop_all_collections(repo):
    repo.dropPermanent('earnings_2014')
    repo.dropPermanent('name_gender_lookup')

repo = get_auth_repo('nikolaj', 'nikolaj')

drop_all_collections(repo)
store_name_gender_lookup(repo)
store_salary_data(repo)
