import datetime
import zipfile
import urllib.request
import io
import json
import prov.model
import pymongo
import uuid

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

# this is a computation
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
    url = 'https://data.cityofboston.gov/resource/ntv7-hwjm.json?'
    retrieve_and_store_json(url, 'earnings_2014', repo)

def drop_all_collections(repo):
    repo.dropPermanent('earnings_2014')
    repo.dropPermanent('name_gender_lookup')

repo = get_auth_repo('nikolaj', 'nikolaj')

drop_all_collections(repo)

startTime = datetime.datetime.now()

store_name_gender_lookup(repo)
store_salary_data(repo)

endTime = datetime.datetime.now()

doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/nikolaj/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/nikolaj/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:getSalaryGenderData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource = doc.entity('bdp:ntv7-hwjm', {'prov:label':'Employee Earnings Report 2014', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

get_earnings = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?'})

doc.wasAssociatedWith(get_earnings, this_script)
doc.used(get_earnings, resource, startTime)

earnings = doc.entity('dat:earnings_2014', {prov.model.PROV_LABEL:'Earnings Report 2014', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(earnings, this_script)
doc.wasGeneratedBy(earnings, get_earnings, endTime)
doc.wasDerivedFrom(earnings, resource, get_earnings, get_earnings, get_earnings)

# get_name_gender_lookup
# preprocess computation results in formatted data
# formatted data gets uploaded

repo.record(doc.serialize()) # Record the provenance document.
print(json.dumps(json.loads(doc.serialize()), indent=4))
# open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()
