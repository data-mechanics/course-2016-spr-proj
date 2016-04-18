from urllib import parse, request
from json import loads, dumps

import pymongo
import prov.model
import datetime
import uuid
#from geopy.geocoders import Nominatim

exec(open('../pymongo_dm.py').read())

def make_provdoc(repo, run_ids, startTime, endTime, queries):
    
    provdoc = prov.model.ProvDocument()
    provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
    provdoc.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
    provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    provdoc.add_namespace('ocd', 'https://api.opencagedata.com/geocode/v1/')

    this_script = provdoc.agent('alg:retrieveservices', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
    input1 = provdoc.entity('dat:seniorservices', {prov.model.PROV_LABEL:'Senior Services', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension': 'json'})
    input2 = provdoc.entity('ocd:reversegeocode', {prov.model.PROV_LABEL:'Reverse Geocode', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension': 'json'})
    output1 = provdoc.entity('dat:seniorservicesgeo', {prov.model.PROV_LABEL:'Senior Services', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension': 'json'})
    output2 = provdoc.entity('dat:servicecenters', {prov.model.PROV_LABEL:'Service Centers', prov.model.PROV_TYPE:'ont:DataSet'})
    
    for i in range(len(run_ids)):
        run_id = run_ids[i]
        this_run = provdoc.activity('log:a'+run_id, startTime, endTime)
        provdoc.wasAssociatedWith(this_run, this_script)
        provdoc.used(this_run, input1, startTime, None, {prov.model.PROV_TYPE:'ont:Computation'})
        provdoc.used(this_run, input2, startTime, None,\
            {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query': "geojson?" + queries[i].split("?")[1]})

        provdoc.wasAttributedTo(output1, this_script)
        provdoc.wasDerivedFrom(output1, input1, this_run, this_run, this_run)
        provdoc.wasDerivedFrom(output1, input2, this_run, this_run, this_run)
        provdoc.wasGeneratedBy(output1, this_run, endTime)

        provdoc.wasAttributedTo(output2, this_script)
        provdoc.wasDerivedFrom(output2, input1, this_run, this_run, this_run)
        provdoc.wasDerivedFrom(output2, input2, this_run, this_run, this_run)
        provdoc.wasGeneratedBy(output2, this_run, endTime)


    if startTime == None:
        plan = open('plan.json','r')
        docModel = prov.model.ProvDocument()
        doc = docModel.deserialize(plan)
        doc.update(provdoc)
        plan.close()
        plan = open('plan.json', 'w')
        plan.write(json.dumps(json.loads(doc.serialize()), indent=4))
        plan.close()
    else:
        repo.record(provdoc.serialize()) 


client = pymongo.MongoClient()
repo = client.repo

##########


startTime = datetime.datetime.now()

with open("auth.json") as temp:
    f = temp.read()

    auth = loads(f)
    user = auth['user']

    repo.authenticate(auth['user'], auth['user'])

    key = auth['service']['opencagegeo']['key']

    servicequery = "http://datamechanics.io/data/jgyou/seniorservices.json"

    returned = request.urlopen(servicequery).read().decode("utf-8")
    services = json.loads(returned)

    queries = []

    for site in services:
        # get lat, lon data

        s = site['Street'] + "," + site['City'] + ", MA" + " " + "0"+ str(site['Zipcode'])

        query = "https://api.opencagedata.com/geocode/v1/geojson?q=" + parse.quote_plus(s) + "&limit=1" \
            + "&pretty=1" + "&countrycode=us" +"&key=" + key

        queries.append(query)
        
        serviceresult =  request.urlopen(query).read().decode("utf-8")
        servicejs = json.loads(serviceresult)
        
        # get lat, lon data
        lat = float(servicejs['features'][0]['geometry']['coordinates'][0])
        lon = float(servicejs['features'][0]['geometry']['coordinates'][1])
        site["latitude"] = lat
        site["longitude"] = lon

    # write to json for use in visualization
    with open("seniorservicesgeo.json", "w") as output:
        output.write(json.dumps(services, indent=4))
        output.close()

        # then add to database
        repo.dropPermanent("servicecenters")
        repo.createPermanent("servicecenters")

        repo[user + ".servicecenters"].insert_many(services)

        endTime = datetime.datetime.now()

        # make prov document

        run_ids = [str(uuid.uuid4()) for i in range(len(services))]

        make_provdoc(repo, run_ids, startTime, endTime, queries)
        make_provdoc(repo, run_ids, None, None, queries)

        repo.logout()




    # for site in repo[user + ".servicecenters"].find():
    #     s = site['Street'] + "," + site['City'] + ", MA" + " " + "0"+ str(site['Zipcode'])

    #     query = "https://api.opencagedata.com/geocode/v1/geojson?q=" + parse.quote_plus(s) + "&limit=1" \
    #         + "&pretty=1" + "&countrycode=us" +"&key=" + key
    #     serviceresult =  request.urlopen(query).read().decode("utf-8")
    #     servicejs = json.loads(serviceresult)

    #     # get lat, lon data
    #     lat = float(servicejs['features'][0]['geometry']['coordinates'][0])
    #     lon = float(servicejs['features'][0]['geometry']['coordinates'][1])

    #     services["latitude"] = lat
    #     services["longitude"] = lon

    #     # then insert coordinates into each entry of collection
    #     repo[user + '.servicecenters'].update({"_id": site["_id"]}, {"$set": {"latitude": lat, "longitude": lon} })

    ## also write new .json file with updated service info, to be used for mapping

    




