'''
makescorecsv.py 

Script that creates csv file based on score output and blockgroup data 
from Housing and Urban Development department. Information in this CSV file
is basis for the scatterplot.
'''
from urllib import request, parse
import json
import pymongo
import prov.model
import datetime
import uuid
import time
import csv
import re

exec(open('../pymongo_dm.py').read())

# prov document uses the original input data set for documentation purposes
# to retain origin of data; however, actual data set retrieved from datamechanics
# directories
def make_provdoc(repo, run_id, startTime, endTime):
    provdoc = prov.model.ProvDocument()
    provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
    provdoc.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
    provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    provdoc.add_namespace('lai', 'http://lai.locationaffordability.info//download_csv.php')        # Location Affordability Index

    this_script = provdoc.agent('alg:makescorecsv', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
    input1 = provdoc.entity('lai:', {prov.model.PROV_LABEL:'Senior Services', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension': 'json'})
    input2 = provdoc.entity('dat:scores', {prov.model.PROV_LABEL:'Distance Scores', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension': 'json'})
    output1 = provdoc.entity('dat:plot', {prov.model.PROV_LABEL:'Plot LAI CSV Data', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension': 'csv'})
    
    this_run = provdoc.activity('log:a'+run_id, startTime, endTime)
    provdoc.wasAssociatedWith(this_run, this_script)
    provdoc.used(this_run, input2, startTime, None, {prov.model.PROV_TYPE:'ont:Computation'})
    provdoc.used(this_run, input1, startTime, None,\
        {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query': "cbsa=14460&geography=blkgrp"})

    provdoc.wasAttributedTo(output1, this_script)
    provdoc.wasDerivedFrom(output1, input1, this_run, this_run, this_run)
    provdoc.wasDerivedFrom(output1, input2, this_run, this_run, this_run)
    provdoc.wasGeneratedBy(output1, this_run, endTime)

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


# retrieve certain elements from the dataset "Location Affordability Index by Census Block Group"
client = pymongo.MongoClient()
repo = client.repo

with open("auth.json") as f:
    auth = json.loads(f.read())
    user = auth['user']

    repo.authenticate(user, user)

    startTime = datetime.datetime.now()

    with request.urlopen("http://datamechanics.io/data/jgyou/lai_data_14460_blkgrps.csv") as f2:
        laiinfo = csv.reader(f2.read().decode("utf-8").splitlines())
        laiarr = []
        headers = next(laiinfo, None)
        
        for row in laiinfo:
            censusblock = int(row[headers.index("blkgrp")].replace("'", ""))
            rai = float(row[headers.index("retail_access_index")])
            laiarr.append((censusblock, rai))
        f2.close()

        # then using score output, get addr, censusblock, etc.
        
        with open("scores.json") as f4:
            scores = json.loads(f4.read())
            output = [[None for j in range(5)] for i in range(len(scores) + 1)]
            for i, s in enumerate(scores):
                addr = s['address']
                censusblock = s['census_block']
                score = s['score']
                output[i + 1][0] = addr
                output[i + 1][1] = censusblock
                output[i + 1][3] = score

                if re.search("[0-9]{5,5}", addr):
                    zipcode = re.findall("[0-9]{5,5}", addr)[0]
                else:
                    zipcode = None

                output[i + 1][2] = zipcode

                result = [rai for (cb, rai) in laiarr if str(cb) in str(censusblock)]

                # if there is no matching census block info, take 
                if len(result) == 0:
                    result2 = [rai for (cb, rai) in laiarr if str(cb)[0:len(str(cb))-1] in str(censusblock)]
                    if len(result2) != 0:
                        output[i + 1][4] = result2[0]
                else:
                    output[i + 1][4] = result[0]

            f4.close()

            # header for csv
            output[0] = ["address", "censusblock", "zipcode", "score", "rai"]

            # write out this information to csv file to be used in d3 plots

            with open("plot.csv", "w") as f3:
                plt = csv.writer(f3)
                plt.writerows(output)
                f3.close()

                endTime = datetime.datetime.now()

                run_id = str(uuid.uuid4())
                make_provdoc(repo, run_id, startTime, endTime)
                make_provdoc(repo, run_id, None, None)
            
        
    



