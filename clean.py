import urllib.request
import json
import pymongo
#import prov.model
import datetime
import uuid

def run(assessments_2014,assessments_2015,approved_permits):

    ### CLEAN ###

    # Delete all records missing a critcal field.
    assessments_2014.delete_many({"parcel_id": {'$exists': False}})
    assessments_2015.delete_many({"pid": {'$exists': False}})
    approved_permits.delete_many({"parcel_id": {'$exists': False}})

    assessments_2014.delete_many({"ptype": {'$exists': False}})
    assessments_2015.delete_many({"ptype": {'$exists': False}})

    assessments_2014.delete_many({"av_bldg": {'$exists': False}})
    assessments_2015.delete_many({"av_bldg": {'$exists': False}})

    assessments_2014.delete_many({"av_land": {'$exists': False}})
    assessments_2015.delete_many({"av_land": {'$exists': False}})

    assessments_2014.delete_many({"av_total": {'$exists': False}})
    assessments_2015.delete_many({"av_total": {'$exists': False}})

    approved_permits.delete_many({"declared_valuation": {'$exists': False}})