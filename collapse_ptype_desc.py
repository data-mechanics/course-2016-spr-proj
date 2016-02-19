# modules
import urllib.request
import json
import pymongo
#import prov.model
import datetime
import uuid

def run(repo, assessments_2014,assessments_2015,approved_permits):

    ptypes = repo.djmcc_jasper.ptypes

    for doc in assessments_2014.find():
        id = doc['_id']
        ptypeNum = doc['ptype']


        descriptions = ptypes.find({'code':ptypeNum})

        d = ''

        for description in descriptions:
            d = description['description']

        assessments_2014.update_one(
            {'_id':doc['_id']},
            {
                "$set": {
                    "ptype": d
                }
            }
        )
