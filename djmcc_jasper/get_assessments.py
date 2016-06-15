##########################################################
# Title:   get_assessments.py
# Authors: Daren McCulley and Jasper Burns
# Purpose: Source 2015 assessment data  
##########################################################

import urllib.request
import json
import dml
import prov.model
import uuid
import datetime

N = 168115    # number of assessment records for 2015
LIMIT = 50000 # Socrata API record return limit

# main()

def run(repo):

    # for provenance
    startTime = datetime.datetime.now()

    print("Fetching 2015 Property Assessments...")

    url = 'https://data.cityofboston.gov/resource/yv8c-t43q.json'
    select = '&$select=ptype,pid,st_num,st_name,st_name_suf,zipcode,av_total,av_bldg,av_land,living_area'
    for i in range(0, 1 + N // LIMIT):
        limit  = '$limit='   + str(LIMIT)
        offset = '&$offset=' + str(i * LIMIT)
        query  = '?' + limit + offset + select
        result = urllib.request.urlopen(url + query).read().decode('utf-8')
        asmts  = json.loads(result)
        repo.djmcc_jasper.assessments.insert_many(asmts)

    # for provenance
    endTime = datetime.datetime.now()

    # provenance:

    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/djmcc_jasper/')
    doc.add_namespace('dat', 'http://datamechanics.io/data/djmcc_jasper/')
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
    doc.add_namespace('log', 'http://datamechanics.io/log#')
    doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

    # agent: script
    agt_script = doc.agent('alg:get_assessments', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    # entity: data source
    res_assessments = doc.entity('bdp:yv8c-t43q', {'prov:label':'Property Assessments 2015', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

    # activity: query (without limit and offset)
    act_query = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?$select=ptype,pid,st_num,st_name,st_name_suf,zipcode,av_total,av_bldg,av_land,living_area'})

    # edges
    doc.wasAssociatedWith(act_query, agt_script)
    doc.used(act_query, res_assessments, startTime)

    # entity: data set
    dat_assessments = doc.entity('dat:assessments', {prov.model.PROV_LABEL:'Property Assessments 2015', prov.model.PROV_TYPE:'ont:DataSet'})
 
    # edges
    doc.wasAttributedTo(dat_assessments, agt_script)
    doc.wasGeneratedBy(dat_assessments, act_query, endTime)
    doc.wasDerivedFrom(dat_assessments, res_assessments, act_query, act_query, act_query)

    repo.record(doc.serialize())

#EOF
