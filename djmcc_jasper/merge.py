# modules
import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid

def run(repo):

    startTime = datetime.datetime.now()

    ptypes = repo.djmcc_jasper.ptypes.find()

    print("Adding use to assessment data...")

    for ptype in ptypes:
        repo.djmcc_jasper.assessments_2014.update_many({'ptype': ptype['code']},
            {'$set': {'use': ptype['use']}} )
        repo.djmcc_jasper.assessments_2015.update_many({'ptype': ptype['code']},
            {'$set': {'use': ptype['use']}} )

    print("Removing 2014 Property Assessments without a known use...")
    
    result = repo.djmcc_jasper.assessments_2014.delete_many({'use': {'$exists': False}})

    print("Removed " + str(result.deleted_count) + " records from 2014 Property Assessments...")

    print("Removing 2015 Property Assessments without a known use...")
    
    result = repo.djmcc_jasper.assessments_2015.delete_many({'use': {'$exists': False}})

    print("Removed " + str(result.deleted_count) + " records from 2015 Property Assessments...")

    endTime = datetime.datetime.now()
    makeProv(startTime,endTime,repo)

def makeProv(startTime,endTime,repo):
    # Create the provenance document describing everything happening
    # in this script. Each run of the script will generate a new
    # document describing that invocation event. This information
    # can then be used on subsequent runs to determine dependencies
    # and "replay" everything. The old documents will also act as a
    # log.

    doc = prov.model.ProvDocument()

    ########## NAMESPACES
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/djmcc_jasper/') # The scripts in <folder>/<filename> format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/djmcc_jasper/') # The data sets in <user>/<collection> format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

    ########## AGENTS: (this script)
    this_script = doc.agent('alg:merge', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    ########## ENTITIES: (mongodb databases we're loading)
    resource_2014 = doc.entity('dat:assessments_2014', {'prov:label':'Property Assessments 2014', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
    resource_2015 = doc.entity('dat:assessments_2015', {'prov:label':'Property Assessments 2015', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
    resource_ptypes = doc.entity('dat:ptypes', {'prov:label':'Massachusetts Property Classifications', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})

    ########## ACTIVITY: RETRIEVE
    retrieve_2014  = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})
    retrieve_2015  = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})
    retrieve_ptypes  = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})

    doc.wasAssociatedWith(retrieve_2014, this_script)
    doc.wasAssociatedWith(retrieve_2015, this_script)
    doc.wasAssociatedWith(retrieve_ptypes, this_script)

    doc.used(retrieve_2014, resource_2014, startTime) # used(activity, entity, time)
    doc.used(retrieve_2015, resource_2015, startTime)
    doc.used(retrieve_ptypes, resource_ptypes, startTime)

    ########## ACTIVITY: ADD DATA FIELD (calling extension for lack of more robust terminology)
    add_used_field_2014  = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Extension'})
    add_used_field_2015  = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Extension'})

    doc.wasAssociatedWith(add_used_field_2014, this_script)
    doc.wasAssociatedWith(add_used_field_2015, this_script)

    doc.used(add_used_field_2014, resource_2014, startTime) # Didn't know if I should put the data field we're drawing from (ptypes) or inputting into. I did both
    doc.used(add_used_field_2014, resource_ptypes, startTime)

    doc.used(add_used_field_2015, resource_2015, startTime)
    doc.used(add_used_field_2015, resource_ptypes, startTime)

    ########## ACTIVITY: DELETE DOCUMENTS (without 'use' info i.e. didn't have appropriate property type codes for the merge)
    delete_2014  = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:DataSet'}) # Didn't know what ontology I should use to reference deleting. Since it changes the dataset, I used DataSet
    delete_2015  = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:DataSet'})

    doc.wasAssociatedWith(delete_2014, this_script)
    doc.wasAssociatedWith(delete_2015, this_script)

    doc.used(delete_2014, resource_2014, startTime)
    doc.used(delete_2015, resource_2015, startTime)

    ########## ENTITIES: Newly created

    # THIS PART MAY NOT BE NECESSARY (or the deletion activity might not be necessary). Wasn't sure if deletion qualified as generating a new dataset.

    new_resource_2014 = doc.entity('dat:assessments_2014', {prov.model.PROV_LABEL:'Property Assessments 2014', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(new_resource_2014, this_script)
    doc.wasGeneratedBy(new_resource_2014, delete_2014, endTime)
    doc.wasDerivedFrom(new_resource_2014, resource_2014, delete_2014, delete_2014, delete_2014)

    new_resource_2015 = doc.entity('dat:assessments_2015', {prov.model.PROV_LABEL:'Property Assessments 2015', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(new_resource_2015, this_script)
    doc.wasGeneratedBy(new_resource_2015, delete_2015, endTime)
    doc.wasDerivedFrom(new_resource_2015, resource_2015, delete_2015, delete_2015, delete_2015)

    repo.record(doc.serialize()) # Record the provenance document.