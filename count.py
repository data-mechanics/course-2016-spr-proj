import urllib.request
import json
import pymongo
#import prov.model
import datetime
import uuid

def run(assessments_2014,assessments_2015,approved_permits):

    ### COUNT ###

    # Create cursors and sort data
    asmts2014 = list(assessments_2014.find().sort('parcel_id', pymongo.ASCENDING))
    asmts2015 = list(assessments_2015.find().sort('pid', pymongo.ASCENDING))
    permits   = list(approved_permits.find().sort('parcel_id', pymongo.ASCENDING))

    total = 0

    # Count permits
    for permit in permits:
        value = float(permit['declared_valuation'])
        total += value

    print("The total value of the building permits requested in 2014 is: " + '${:,.2f}'.format(total))

    n2014    = len(asmts2014)
    n2015    = len(asmts2015)
    nPermits = len(permits)

    print("Number of assessed properties in 2014: " + str(n2014))
    print("Number of assessed properties in 2015: " + str(n2015))
    print("Number of approved permits from 1-1-2014 through 12-31-2014: " + str(nPermits))

    count = 0
    i = 0
    j = 0

    while i < n2014 and j < n2015:
        id2014 = asmts2014[i]['parcel_id']
        id2015 = asmts2015[j]['pid']
        minId = min(id2014, id2015)
        if id2014 == id2015:
            count += 1
            i += 1
            j += 1
        elif id2014 == minId:
            i += 1
        else:
            j += 1

    print("Number of overlapping assessments between 2014 and 2015: " + str(count))

    count = 0
    i = 0
    j = 0
    k = 0

    while i < n2014 and j < n2015 and k < nPermits:
        id2014 = asmts2014[i]['parcel_id']
        id2015 = asmts2015[j]['pid']
        idP = permits[k]['parcel_id']
        minId = min(id2014, id2015, idP)
        if id2014 == id2015 and id2015 == idP:
            count += 1
            i += 1
            j += 1
            k += 1
        elif id2014 == minId:
            i += 1
        elif id2015 == minId:
            j += 1
        else:
            k += 1

    print("Number of parcel_ids that were assessed in both 2014 and 2015, that applied for a permit: " + str(count))


