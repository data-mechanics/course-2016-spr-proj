##########################################################
# Title:   get_apartments.py
# Authors: Daren McCulley and Jasper Burns
# Purpose: Scrape PadMapper marker data 
# Credit:  Jeff Kaufman - for a code model and inspiration
#          https://github.com/jeffkaufman/apartment_prices
##########################################################

import urllib.request
import json
import pymongo
import time
import prov.model
import uuid
import datetime

# search box (encompasses Boston):

MIN_LAT =  42.2273878
MAX_LAT =  42.4351936
MIN_LON = -71.1951745
MAX_LON = -70.9224046

# price range and granularity

MIN_RENT = 1000
MAX_RENT = 6000
STEP     = 50

# default query parameters

DEFAULTS = {
    'westLong': MIN_LON,
    'eastLong': MAX_LON,
    'southLat': MIN_LAT,
    'northLat': MAX_LAT,
    'cities': 'false',
    'showPOI': 'false',
    'limit': 3000,
    'searchTerms': '',
    'maxPricePerBedroom': 6000,
    'minBR': 0,
    'maxBR': 10,
    'minBA': 1,
    'maxAge': 7,
    'imagesOnly': 'false',
    'phoneReq': 'false',
    'cats': 'false',
    'dogs': 'false',
    'noFee': 'false',
    'showSubs': 'true',
    'showNonSubs': 'true',
    'showRooms': 'false',
    'showVac': 'false',
    'userId': -1,
    'cl': 'true',
    'pl': 'true',
    'aptsrch': 'true',
    'rnt': 'true',
    'af': 'true',
    'rltr': 'true',
    'ood': 'true',
    'zoom': 15,
    'favsOnly': 'false',
    'onlyHQ': 'true',
    'showHidden': 'false',
    'am': 'false',
    'airbnb': 'false',
    'workplaceLat': 0,
    'workplaceLong': 0,
    'maxTime': 0
}

# query(rent) returns PadMapper markers within the range(rent, rent + STEP - 1)

def query(rent):

    # build the web query
    url_prefix = 'https://www.padmapper.com/reloadMarkersJSON.php'
    url = '%s?%s' % (url_prefix, '&'.join( '%s=%s' % (k,v) for (k,v) in DEFAULTS.items()))
    url += 'minRent=' + str(rent) + '&maxRent=' + str(rent + STEP - 1)

    try:
        markers = json.loads(urllib.request.urlopen(url).read().decode('utf-8'))
    
    except Exception as e:
        print('ERROR:', e)
        print('URL: ' + url)
        return []

    return markers

# main()

def run(repo):

    # for provenance
    startTime = datetime.datetime.now()

    for rent in range(MIN_RENT, MAX_RENT + STEP, STEP):

        # note apartments stored under this rent are
        # within the range from rent to rent + STEP - 1
        print('Fetching apartments listed on PadMapper for $%s...' % rent)
        
        markers = query(rent)
        
        print(str(len(markers)) + ' apartments found...')
        
        for marker in markers:
            repo.djmcc_jasper.apartments.insert_one(marker)
            repo.djmcc_jasper.apartments.update_one({'_id': marker['_id']}, {'$set': {'rent': rent}})

        time.sleep(3) # being a good web-citizen

    print (str(repo.djmcc_jasper.apartments.count()) + ' apartments found in total...')

    # for provenance
    endTime = datetime.datetime.now()

    # provenance:

    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/djmcc_jasper/')
    doc.add_namespace('dat', 'http://datamechanics.io/data/djmcc_jasper/')
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
    doc.add_namespace('log', 'http://datamechanics.io/log#')
    doc.add_namespace('pdm', 'https://www.padmapper.com/')

    # agent: script
    agt_script = doc.agent('alg:get_apartments', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    # entity: data source
    
    res_padmapper = doc.entity('pdm:reloadMarkersJSON', {'prov:label':'Markers', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'php'})

    # activity: query
    act_query = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?dogs=false&southLat=42.2273878&searchTerms=&rltr=true&imagesOnly=false&cl=true&showHidden=false&cats=false&eastLong=-70.9224046&maxTime=0&favsOnly=false&showSubs=true&showNonSubs=true&showRooms=false&noFee=false&am=false&limit=3000&phoneReq=false&pl=true&showPOI=false&ood=true&aptsrch=true&showVac=false&maxBR=10&userId=-1&westLong=-71.1951745&minBA=1&onlyHQ=true&workplaceLong=0&rnt=true&airbnb=false&maxPricePerBedroom=6000&af=true&minBR=0&cities=false&workplaceLat=0&northLat=42.4351936&maxAge=7&zoom=15minRent=<minRent>&maxRent=<maxRent>'})

    # edges
    doc.wasAssociatedWith(act_query, agt_script)
    doc.used(act_query, res_padmapper, startTime)

    # entity: data set
    dat_apartments = doc.entity('dat:apartments', {prov.model.PROV_LABEL:'Apartments', prov.model.PROV_TYPE:'ont:DataSet'})

    # edges
    doc.wasAttributedTo(dat_apartments, agt_script)
    doc.wasGeneratedBy(dat_apartments, act_query, endTime)
    doc.wasDerivedFrom(dat_apartments, res_padmapper, act_query, act_query, act_query)

    repo.record(doc.serialize())

#EOF
