import urllib.request
import json
import pymongo
#import prov.model
import datetime
import uuid

def run(repo):
    ### GET ###

    url = 'https://data.cityofboston.gov/resource/qz7u-kb7x.json?'
    select = '&$select=parcel_id,ptype,av_land,av_bldg,av_total'
    where = '&$where=zipcode%20=%20%2702134%27'
    limit = '$limit=50000'
    response = urllib.request.urlopen(url+limit+select+where).read().decode('utf-8')
    r1 = json.loads(response)
    s1 = json.dumps(r1, sort_keys=True, indent=2)
    repo.dropPermanent("assessment_2014")
    repo.createPermanent("assessment_2014")
    repo["djmcc_jasper.assessment_2014"].insert_many(r1)

    url = 'https://data.cityofboston.gov/resource/yv8c-t43q.json?'
    select = '&$select=pid,ptype,av_land,av_bldg,av_total'
    where = '&$where=zipcode%20=%20%2702134%27'
    limit = '$limit=50000'
    response = urllib.request.urlopen(url+limit+select+where).read().decode("utf-8")
    r2 = json.loads(response)
    s2 = json.dumps(r2, sort_keys=True, indent=2)
    repo.dropPermanent("assessment_2015")
    repo.createPermanent("assessment_2015")
    repo['djmcc_jasper.assessment_2015'].insert_many(r2)

    url = 'https://data.cityofboston.gov/resource/msk6-43c6.json?'
    select = '&$select=parcel_id,declared_valuation'
    where = '&$where=zip=%2702134%27%20AND%20issued_date%20%3E%20%271013-12-31%27%20AND%20issued_date%20%3C%20%272015-01-01%27'
    limit = '$limit=50000'
    response = urllib.request.urlopen(url+limit+select+where).read().decode("utf-8")
    r3 = json.loads(response)
    s3 = json.dumps(r3, sort_keys=True, indent=2)
    repo.dropPermanent("approved_permits")
    repo.createPermanent("approved_permits")
    repo['djmcc_jasper.approved_permits'].insert_many(r3)

    with open('ptype.json') as ptypes_raw:
        ptypes = json.load(ptypes_raw)
        ptypes_raw.close()
        repo.dropPermanent("ptype")
        repo.dropPermanent("ptypes")
        repo.createPermanent("ptypes")
        repo['djmcc_jasper.ptypes'].insert_many(ptypes)