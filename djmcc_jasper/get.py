import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid

# Total number of records as of some date/time:
# Future update - Could use Socrata to count...

N2014 = 164091
N2015 = 168115
NPerm = 267427

# Socrata API record return limit:
LIMIT = 50000

def run(repo):

    # for provenance
    startTime = datetime.datetime.now()

    print("Fetching 2014 Property Assessments...")

    # 2014 Property Assessments
    url = 'https://data.cityofboston.gov/resource/qz7u-kb7x.json'
    select = '&$select=parcel_id,ptype,av_land,av_bldg,av_total,zipcode,gross_tax'
    for i in range(0, 1 + N2014 // LIMIT):
        limit  = '$limit='   + str(LIMIT)
        offset = '&$offset=' + str(i * LIMIT)
        query  = '?' + limit + offset + select
        result = urllib.request.urlopen(url + query).read().decode('utf-8')
        r      = json.loads(result)
        repo["djmcc_jasper.assessments_2014"].insert_many(r)

    print("Fetching 2015 Property Assessments...")

    # 2015 Property Assessments
    url = 'https://data.cityofboston.gov/resource/yv8c-t43q.json'
    select = '&$select=pid,ptype,av_land,av_bldg,av_total,zipcode,gross_tax'
    for i in range(0, 1 + N2015 // LIMIT):
        limit  = '$limit='   + str(LIMIT)
        offset = '&$offset=' + str(i * LIMIT)
        query  = '?' + limit + offset + select
        result = urllib.request.urlopen(url + query).read().decode('utf-8')
        r      = json.loads(result)
        repo["djmcc_jasper.assessments_2015"].insert_many(r)

    print("Fetching Approved Permits...")

    # Approved Permits
    url = 'https://data.cityofboston.gov/resource/msk6-43c6.json'
    select = '&$select=parcel_id,declared_valuation,location,occupancytype,zip,issued_date'
    for i in range(0, 1 + NPerm // LIMIT):
        limit  = '$limit='   + str(LIMIT)
        offset = '&$offset=' + str(i * LIMIT)
        query  = '?' + limit + offset + select
        result = urllib.request.urlopen(url + query).read().decode('utf-8')
        r      = json.loads(result)
        repo["djmcc_jasper.approved_permits"].insert_many(r)

    print("Fetching Property Types...")
    
    # Property Types
    url = 'https://data-mechanics.s3.amazonaws.com/djmcc_jasper/ptype.json'
    result = urllib.request.urlopen(url).read().decode('utf-8')
    r = json.loads(result)
    repo["djmcc_jasper.ptypes"].insert_many(r)

    endTime = datetime.datetime.now()

    # Create the provenance document describing everything happening
    # in this script. Each run of the script will generate a new
    # document describing that invocation event. This information
    # can then be used on subsequent runs to determine dependencies
    # and "replay" everything. The old documents will also act as a
    # log.

    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/djmcc_jasper/') # The scripts in <folder>/<filename> format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/djmcc_jasper/') # The data sets in <user>/<collection> format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#')

    # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.

    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

    this_script = doc.agent('alg:get', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    resource_2014 = doc.entity('bdp:qz7u-kb7x', {'prov:label':'Property Assessments 2014', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    resource_2015 = doc.entity('bdp:yv8c-t43q', {'prov:label':'Property Assessments 2015', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    resource_permits = doc.entity('bdp:msk6-43c6', {'prov:label':'Approved Building Permits', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

    # manually generated dataset: https://www.cityofboston.gov/images_documents/MA_OCCcodes_tcm3-16189.pdf
    # resource_ptypes = doc.entity('???:ptype', {'prov:label':'Massachusetts Property Classifications', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})

    # leaves out limit and offset parameters:
    query_2014    = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?$select=parcel_id,ptype,av_land,av_bldg,av_total,zipcode,gross_tax'})
    query_2015    = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?$select=parcel_id,ptype,av_land,av_bldg,av_total,zipcode,gross_tax'})
    query_permits = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?$select=parcel_id,declared_valuation,location,occupancytype,zip,issued_date'})

    # query_ptypes  = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})

    doc.wasAssociatedWith(query_2014, this_script)
    doc.wasAssociatedWith(query_2015, this_script)
    doc.wasAssociatedWith(query_permits, this_script)

    # doc.wasAssociatedWith(query_ptypes, this_script)

    # used(activity, entity, time)
    doc.used(query_2014, resource_2014, startTime)
    doc.used(query_2015, resource_2015, startTime)
    doc.used(query_permits, resource_permits, startTime)

    # doc.used(query_ptypes, resource_ptypes, startTime)

    # stored datasets

    prov_assessments_2014 = doc.entity('dat:assessments_2014', {prov.model.PROV_LABEL:'Property Assessments 2014', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(prov_assessments_2014, this_script)
    doc.wasGeneratedBy(prov_assessments_2014, query_2014, endTime)
    doc.wasDerivedFrom(prov_assessments_2014, resource_2014, query_2014, query_2014, query_2014)

    prov_assessments_2015 = doc.entity('dat:assessments_2015', {prov.model.PROV_LABEL:'Property Assessments 2015', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(prov_assessments_2015, this_script)
    doc.wasGeneratedBy(prov_assessments_2015, query_2015, endTime)
    doc.wasDerivedFrom(prov_assessments_2015, resource_2015, query_2015, query_2015, query_2015)

    prov_permits = doc.entity('dat:approved_permits', {prov.model.PROV_LABEL:'Approved Building Permits', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(prov_permits, this_script)
    doc.wasGeneratedBy(prov_permits, query_permits, endTime)
    doc.wasDerivedFrom(prov_permits, resource_permits, query_permits, query_permits, query_permits)

    # prov_ptypes = doc.entity('dat:ptypes', {prov.model.PROV_LABEL:'Massachusetts Property Classifications', prov.model.PROV_TYPE:'ont:DataSet'})
    # doc.wasAttributedTo(prov_ptypes, this_script)
    # doc.wasGeneratedBy(prov_ptypes, query_ptypes, endTime)
    # doc.wasDerivedFrom(prov_ptypes, resource_ptypes, query_ptypes, query_ptypes, query_ptypes)

    repo.record(doc.serialize()) # Record the provenance document.
    # print(json.dumps(json.loads(doc.serialize()), indent=4))
    # print(doc.get_provn())

#EOF