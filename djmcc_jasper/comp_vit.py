##########################################################
# Title:   comp_vit.py
# Authors: Daren McCulley and Jasper Burns
# Purpose: For every zip code in Boston’s assessments
#          database of acceptable size, it tallies the 
#          residential and commercial property count, and 
#          finds the percent composition of commercial 
#          properties. It also calculates a vitality score 
#          by querying Yelp to find the average amount of 
#          reviews per business in a the zip code
##########################################################

import json
import pymongo
import datetime
import uuid
import rauth
import time
import prov.model

# main()

def run(repo,authFile):

    # For Prov
    startTime = datetime.datetime.now()

    assessments = repo.djmcc_jasper.assessments
    zipcodes = repo.djmcc_jasper.assessments.distinct('zipcode')

    # Drops zipcodes we don't care about (i.e. don't have enough properties)
    for zipcode in zipcodes:
        if (assessments.find({'zipcode': zipcode}).count() < 100):
            print("Removing " + zipcode + " for too few entries")
            zipcodes.remove(zipcode)

    # For logging purposes, to see what percent done we are
    count = 0

    # Add data to database
    for zipcode in zipcodes:

            # compositions[0] is ptypeList, compositions[1] is useList
            compositions   = composition(assessments, zipcode)
            p_com = percent_com(compositions)

            post = {    "zipcode": zipcode,
                        "vitality": vitality(zipcode,authFile),
                        "residential": compositions[0],
                        "commercial": compositions[1],
                        "percent_com": p_com,
                    }

            repo.djmcc_jasper.web_data.insert_one(post)

            # Logging
            count += 1
            print('{:.2%}'.format(count/float(len(zipcodes))) + "\tJust added " + zipcode + " to web_data collection")

    # Create a JSON file for the visualizaztion
    generateJSON(repo)

    # For Prov
    endTime = datetime.datetime.now()
    makeProv(startTime,endTime,repo)

def composition(assessments, zipcode):

    # List of all the property types and compositions: [[use,ptype1,composition],[use,ptype2,composition],...]
    residentialCount = 0
    commercialCount = 0

    # For all docs in the current assessments
    for doc in assessments.find({'zipcode':zipcode}):
        ptype = doc['ptype']

        if ptype[:1] == '1':
            residentialCount += 1
        if ptype[:1] == '3':
            commercialCount += 1

    return [residentialCount,commercialCount]

def vitality(zipcode,authFile):
    review_count = 0
    counter = 0.0

    #0-19
    params = get_search_parameters(zipcode, 0)
    data = get_results(params,authFile)
    total = data.get("total")
    for b in data.get("businesses"):
        review_count += (b.get("review_count"))
        counter += 1

    #20-300
    for offset in [20*x for x in range(1,15)]: #300 businesses
        params = get_search_parameters(zipcode,offset)
        data = get_results(params,authFile)
        for b in data.get("businesses"):
            review_count += (b.get("review_count"))
            counter += 1

    # Vitality score is the average amount of reviews for the first 300 entries
    avg = review_count / counter
    print(str(review_count) + " " + str(counter) + " " + str(avg))



    # Altruistic rate limiting
    time.sleep(0.5)

    if avg > 0:
        return avg
    else:
        return "err"

def get_search_parameters(zipcode,offset):
  #See the Yelp API for more details
  params = {}
  #params["term"] = "restaurant"
  #params["ll"] = "{},{}".format(str(lat),str(long))
  params["location"] = zipcode
  params["radius_filter"] = "2000" #1.25 miles
  params["sort"] = "0"
  params["limit"] = "20"
  params["offset"] = str(offset)

  return params

def get_results(params,authFile):
    # Obtain these from Yelp's manage access page
    consumer_key = authFile['consumer_key']
    consumer_secret = authFile['consumer_secret']
    token = authFile['token']
    token_secret = authFile['token_secret']

    session = rauth.OAuth1Session(
        consumer_key=consumer_key
        , consumer_secret=consumer_secret
        , access_token=token
        , access_token_secret=token_secret)

    request = session.get("http://api.yelp.com/v2/search", params=params)

    # Transforms the JSON API response into a Python dictionary
    data = request.json()
    session.close()

    return data

def percent_com(compositions):
    # r = residential + apartments
    r = float(compositions[0])
    c = float(compositions[1])
    t = r + c
    p_com = 0
    if t != 0:
        p_com = c / t
    return p_com

def generateJSON(repo):
    with open('part2/comp_vit_data.json', 'w') as f:
        f.write('[')

        for record in repo.djmcc_jasper.web_data.find().limit(repo.djmcc_jasper.web_data.count() - 1):
            f.write('\t{"vitality": ' + str(record['vitality']) + ',' + \
                    '"residential": ' + str(record['residential']) + ',' + \
                    '"commercial": ' + str(record['commercial']) + ',' + \
                    '"percent_com": ' + str(record['percent_com']) + ',' + \
                    '"zipcode": "' + str(record['zipcode']) + '"},\n')

        for record in repo.djmcc_jasper.web_data.find().skip(repo.djmcc_jasper.web_data.count() - 1):
            f.write('\t{"vitality": ' + str(record['vitality']) + ',' + \
                    '"residential": ' + str(record['residential']) + ',' + \
                    '"commercial": ' + str(record['commercial']) + ',' + \
                    '"percent_com": ' + str(record['percent_com']) + ',' + \
                    '"zipcode": "' + str(record['zipcode']) + '"}\n]')

    with open('part2/comp_vit_data.json', 'r') as f:
        filedata = f.read()
        newdata = filedata.replace("u\'", "\"")

    with open('part2/comp_vit_data.json', 'w') as f:
        f.write(newdata)


    with open('part2/comp_vit_data.json', 'r') as f:
        filedata = f.read()
        newdata = filedata.replace("\'", "\"")

    with open('part2/comp_vit_data.json', 'w') as f:
        f.write(newdata)

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
    doc.add_namespace('ylp', 'http://api.yelp.com/v2/')

    ########## AGENTS: (this script)
    this_script = doc.agent('alg:comp_vit', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    ########## ENTITIES: (mongodb databases we're loading)
    resource_2015 = doc.entity('dat:assessments', {'prov:label':'Property Assessments 2015', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
    yelp_api = doc.entity('ylp:search', {'prov:label': 'Yelp API', prov.model.PROV_TYPE: 'ont:DataSet','ont:Extension': 'json'})

    ########## ACTIVITY: RETRIEVE FROM YELP
    retrieve_yelp  = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})

    doc.wasAssociatedWith(retrieve_yelp, this_script)
    doc.used(retrieve_yelp, yelp_api, startTime)

    ########## ACTIVITY: COMPUTE
    web_data_computation  = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

    doc.wasAssociatedWith(web_data_computation, this_script)

    doc.used(web_data_computation, resource_2015, startTime)
    doc.used(web_data_computation, yelp_api, startTime)


    ########## ENTITIES: New web_data
    db_comp_vit = doc.entity('dat:web_data', {prov.model.PROV_LABEL:'Composition and Vitality Data', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(db_comp_vit, this_script)
    doc.wasGeneratedBy(db_comp_vit, web_data_computation, endTime)
    doc.wasDerivedFrom(db_comp_vit, resource_2015, web_data_computation, web_data_computation, web_data_computation)
    doc.wasDerivedFrom(db_comp_vit, yelp_api, retrieve_yelp, retrieve_yelp, retrieve_yelp)

    repo.record(doc.serialize()) # Record the provenance document.
