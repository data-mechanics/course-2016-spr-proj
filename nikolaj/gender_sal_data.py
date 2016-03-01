import datetime
import zipfile
import urllib.request
import io
import json
import prov.model
import pymongo
import uuid
import utils
exec(open('../pymongo_dm.py').read())

def get_auth_repo(uname, pwd):
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
    # found in the zip archive.
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
    url = 'https://data.cityofboston.gov/resource/54s2-yxpg.json'
    retrieve_and_store_json(url, 'earnings_2013', repo)
    url = 'https://data.cityofboston.gov/resource/ntv7-hwjm.json'
    retrieve_and_store_json(url, 'earnings_2014', repo)

def drop_all_collections(repo):
    repo.dropPermanent('earnings_2013')
    repo.dropPermanent('earnings_2013_avg_by_gender')
    repo.dropPermanent('earnings_2013_combined')
    repo.dropPermanent('earnings_2013_reduced_by_name')
    repo.dropPermanent('earnings_2014')
    repo.dropPermanent('earnings_2014_avg_by_gender')
    repo.dropPermanent('earnings_2014_combined')
    repo.dropPermanent('earnings_2014_reduced_by_name')
    repo.dropPermanent('name_gender_lookup')

def run_data_retrieval(repo):
    drop_all_collections(repo)
    store_name_gender_lookup(repo)
    store_salary_data(repo)
    
def to_prov(startTime, endTime):
    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/nikolaj/') # The scripts in <folder>/<filename> format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/nikolaj/') # The data sets in <user>/<collection> format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
    doc.add_namespace('ssa', 'https://www.ssa.gov/oact/babynames/')

    this_script = doc.agent('alg:gender_sal_data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    # EARNINGS 2013
    earnings_2013_resource = doc.entity('bdp:54s2-yxpg', {'prov:label':'Employee Earnings Report 2013', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    get_earnings_2013 = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})

    doc.wasAssociatedWith(get_earnings_2013, this_script)
    doc.used(get_earnings_2013, earnings_2013_resource, startTime)

    earnings_2013 = doc.entity('dat:earnings_2013', {prov.model.PROV_LABEL:'Earnings Report 2013', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(earnings_2013, this_script)
    doc.wasGeneratedBy(earnings_2013, get_earnings_2013, endTime)
    doc.wasDerivedFrom(earnings_2013, earnings_2013_resource, get_earnings_2013, get_earnings_2013, get_earnings_2013)

    # EARNINGS 2014
    earnings_2014_resource = doc.entity('bdp:ntv7-hwjm', {'prov:label':'Employee Earnings Report 2014', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    get_earnings_2014 = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'Retrieve Earnings Report 2014', prov.model.PROV_TYPE:'ont:Retrieval'})

    doc.wasAssociatedWith(get_earnings_2014, this_script)
    doc.used(get_earnings_2014, earnings_2014_resource, startTime)

    earnings_2014 = doc.entity('dat:earnings_2014', {prov.model.PROV_LABEL:'Earnings Report 2014', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(earnings_2014, this_script)
    doc.wasGeneratedBy(earnings_2014, get_earnings_2014, endTime)
    doc.wasDerivedFrom(earnings_2014, earnings_2014_resource, get_earnings_2014, get_earnings_2014, get_earnings_2014)

    # NAME GENDER LOOKUP
    name_gender_resource = doc.entity('ssa:names', {'prov:label':'National Data on the relative frequency of given names', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})
    get_name_gender = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})

    doc.wasAssociatedWith(get_name_gender, this_script)
    doc.used(get_name_gender, name_gender_resource, startTime)

    name_gender = doc.entity('dat:name_gender_lookup', {prov.model.PROV_LABEL:'Name Gender Lookup', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(name_gender, this_script)
    doc.wasGeneratedBy(name_gender, get_name_gender, endTime)
    doc.wasDerivedFrom(name_gender, name_gender_resource, get_name_gender, get_name_gender, get_name_gender)
    return doc

@utils.timestamped
def run():
    repo = get_auth_repo('nikolaj', 'nikolaj')
    run_data_retrieval(repo)
    return
