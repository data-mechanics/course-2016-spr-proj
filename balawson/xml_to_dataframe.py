# coding: utf-8
###############################################################
####   import dependancies       
###############################################################

import pandas as pd
import dml, datetime, uuid
import prov.model
import random
import xml.etree.ElementTree as xmltree

###############################################################
####    parse the xml !!!!!       
###############################################################
def xml_to_dataframe(xml_file_name='BostonNetwork/outputs/roadnetwork.xml'):
    roads, lat, lng, population, users = [], [] , [] , [] , [] #lists to store the information
    e = xmltree.parse(xml_file_name).getroot()
    for intersection in e.findall('intersection'):
        children = intersection.getchildren() 
        r = ''
        for child in children:
            if child.get('name'):
                if len(r) < 1:
                    r += ( child.get('name'))
                else:
                    r += ', ' + ( child.get('name'))
            elif child.get('num') :
                pop = child.get('num')
            elif child.get('ulist') :
                user = child.get('ulist')
            else:
                pass
        users += [user]
        lat += [intersection.get('lat')]
        lng += [intersection.get('long')]
        roads.append(r)
        population.append(pop)

    ###############################################################
    ####    make dataframe
    ###############################################################

    d = {'roads'    : pd.Series(roads) ,
        'lat'       : pd.Series(lat)      ,
        'lng'       : pd.Series(lng)      ,
        'population': pd.Series(population) , 
        'users': pd.Series(users) , 
        }

    df = pd.DataFrame(d)
    return df
#df.to_csv('intersectionspopulation.csv')

###############################################################
####    dump dataframe
###############################################################
def insert_df_to_mongo(df, collection_name):
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('balawson', 'balawson')

    #collection_name = 'twitter-intersection'
    records = json.loads(df.T.to_json()).values()
    repo.dropPermanent(collection_name)
    repo.createPermanent(collection_name)
    repo['balawson.' + collection_name].insert_many(records)
