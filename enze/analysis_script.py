
# coding: utf-8

# In[1]:

import csv
import datetime
import json
import pymongo
import prov.model
import urllib.request
import uuid
import stateplane
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import seaborn as sns
import xml.etree.ElementTree as ET
from pyproj import Proj
from pandas import Series, DataFrame
from datetime import datetime
from sklearn import preprocessing

#pd.__version__

get_ipython().magic('matplotlib inline')


# # Data Transformation

# In[2]:

# import the Excel file: Boston Bike Collision Database
df_collision = pd.read_excel("data/Final Bike Collision Database.xlsx")


# In[3]:

# import the csv file: Existing Bike Network Database
df_lane = pd.read_csv("data/Existing_Bike_Network.csv")


# In[4]:

# import the csv file: City of Boston Street Segments Database
df_street = pd.read_csv("data/Boston_Segments.csv")


# In[5]:

# df_collision.info()
# df_collision.head(10)
# df_lane.info()
# df_street.info()

# drop rows of Pandas dataframe whose value of coordinates are NaN
df_collision = df_collision[np.isfinite(df_collision['XFINAL'])]
df_lane = df_lane[df_lane['STREET_NAM'] != " "]
df_street = df_street[df_street['ST_TYPE'].isnull()==False]


# In[2]:

def extract_intersections(osm, verbose=False):
    # http://stackoverflow.com/questions/14716497/how-can-i-find-a-list-of-street-intersections-from-openstreetmap-data
    # This function takes an osm file as an input. It then goes through each xml 
    # element and searches for nodes that are shared by two or more ways.
    # Parameter:
    # - osm: An xml file that contains OpenStreetMap's map information
    # - verbose: If true, print some outputs to terminal.
    # 
    # Ex) extract_intersections('WashingtonDC.osm')
    #
    tree = ET.parse(osm)
    root = tree.getroot()
    counter = {}
    for child in root:
        if child.tag == 'way':
            for item in child:
#                 if item.tag == 'tag' and item.attrib['k'] == 'name':
#                     print(item.attrib['v'])
                if item.tag == 'nd':
                    nd_ref = item.attrib['ref']
                    if not nd_ref in counter:
                        counter[nd_ref] = 0
                    counter[nd_ref] += 1

    # Find nodes that are shared with more than one way, which
    # might correspond to intersections
    intersections = filter(lambda x: counter[x] > 1,  counter)

    # Extract intersection coordinates
    # You can plot the result using this url.
    # http://www.darrinward.com/lat-long/
    intersection_coordinates = []
    for child in root:
        if child.tag == 'node' and child.attrib['id'] in intersections:
            coordinate = child.attrib['lat'] + ',' + child.attrib['lon']
            if verbose:
                print(coordinate)
            intersection_coordinates.append(coordinate)

    return intersection_coordinates


# In[4]:

# extract_intersections("../boston_massachusetts.osm")


# In[59]:

def extract_intersection(east, north):
    # This function takes two inputs: easting and northing of location in feet
    # Finds the nearest street and the next crossing street for a given lat/lng pair. 
    # findNearestIntersectionOSM API
    # http://www.geonames.org/maps/osm-reverse-geocoder.html
    # Ex) extract_intersections(42.3516652, -071.1212753)
    #
    # 1 foot = 0.3048 meter
    conv = 0.3048
    (lat, lon) = stateplane.to_latlon(east*conv, north*conv, fips='2001')
    base_url = "http://api.geonames.org/findNearestIntersectionOSMJSON?"
    request = base_url + "lat=" + str(lat) + "&lng=" + str(lon) + "&username=andrewenze"
    response = urllib.request.urlopen(request).read().decode("utf-8")
    r = json.loads(response)
#     s = json.dumps(r, sort_keys=True, indent=2)
    str1 = r['intersection']
    str2 = r['intersection']
    return (str1['street1'], str2['street2'])

def get_intersection():
    # This function traverse thru each entry of bike collision database
    # and collect the 2 roads that intersect by callin extract_intersection function
    # Save the results to interseciton.csv
    #
    with open('intersection.csv', 'w', newline='') as fp:
        a = csv.writer(fp, delimiter=',')
        for idx, row in df_collision.iterrows():
            str1, str2 = extract_intersection(row.XFINAL, row.YFINAL)
            a.writerows([[str1, str2]])


# In[6]:

# import the csv file of the intersection database we just collected
df_intersection = pd.read_csv("data/intersection.csv")


# In[7]:

df_intersection.head(5)
df_intersection.info()


# In[8]:

def get_bike_lane():
    # This function traverse thru each entry of the existing bike lane database
    # and collect the bike lanes that has bike lane
    #
    bike_lane = {}
    for idx, row in df_lane.iterrows():
        name = row.STREET_NAM
        if name not in bike_lane:
            bike_lane[name] = 1
    return bike_lane


# In[9]:

# Roads that have bike lane
bike_lane = get_bike_lane()


# In[10]:

def add_bike_lane():
    # This function takes the existing intersection database
    # and check if the given 2 streets on each entry have bike lanes
    # according to the existing bike lane database
    #
    for idx, row in df_intersection.iterrows():
        if row.STREET1 in bike_lane:
            df_intersection.ix[idx,'1HASLANE'] = 1
        else:
            df_intersection.ix[idx,'1HASLANE'] = 0
        if row.STREET2 in bike_lane:
            df_intersection.ix[idx,'2HASLANE'] = 1
        else:
            df_intersection.ix[idx,'2HASLANE'] = 0
#     print(df_intersection.head(5))


# In[11]:

add_bike_lane()
df_intersection.head(5)


# In[98]:

# Save the Pandas Dataframe to csv for SMT solver
df_intersection.to_csv('data/intersection_lane.csv',index=False)


# In[12]:

def get_collision_street():
    # This function takes the existing Boston Bike Collision database
    # return a list of the street that the collision happens
    #
    st = []
    for idx, row in df_intersection.iterrows():
        st1 = row.STREET1
        if st1 not in st:
            st.append(st1)
    return st


# In[13]:

collision_street = get_collision_street()


# In[14]:

st_type = {'XWY':'Xwy','BCH':'Beach','IS':'Island','PSGE':'Passage','VW':'View','TERS':'','CSWY':'Causeway','CRES':'Crescent','GDN':'Garden','GDNS':'Gardens','GRN':'Green','DM':'Dam','CIRT':'Circle','EXT':'Extension','ST':'Street','AVE':'Avenue','RD':'Road','PL':'Place','WAY':'Way','TER':'Terrace','BLVD':'Boulevard','CT':'Court','PKWY':'Parkway','DR':'Drive','HWY':'Highway','SQ':'Square','PARK':'Park','CIR':'Circle','TPKE':'Turnpike','LN':'Lane','FWY':'Freeway','BRG':'Bridge','SKWY':'Skyway','ROW':'Row','PATH':'Path','PLZ':'Plaza','ALY':'Alley','MALL':'Mall','WHRF':'Whrf', 'DRWY':'Driveway'}
def street_length():
    # This function takes the existing Boston Road/Street database
    # and filter for the street length information
    #
    street_len = {}
    for idx, row in df_street.iterrows():
        name = row.ST_NAME
        st = row.ST_TYPE
        street = str(name) + " " + str(st_type[st])
        if street in collision_street:
            if street not in street_len:
                street_len[street] = row.SHAPESTLength
            else:
                street_len[street]+=row.SHAPESTLength
    return street_len


# In[15]:

street_length = street_length()
# df_street.ST_TYPE.value_counts()


# # Data Visualizations

# ### Question: What are the streets (with bike lane vs. without bike lane) that has the most amount of incidents?

# In[219]:

# Overall most incident-frequent street
df_intersection.STREET1.value_counts().head(20)


# In[222]:

df_intersection.STREET1.value_counts().head(20).plot.bar(title="Top 20 Collision Streets")


# In[114]:

# Most incident-frequent street with a bike lane
df_intersection[df_intersection['1HASLANE'] == 1].STREET1.value_counts().head(20)


# In[223]:

df_intersection[df_intersection['1HASLANE'] == 1].STREET1.value_counts().head(20).plot.bar(title="Top 20 Collision Streets w/ Bike Lane")


# In[115]:

# Most incident-frequent street without a bike lane
df_intersection[df_intersection['1HASLANE'] == 0].STREET1.value_counts().head(20)


# In[224]:

df_intersection[df_intersection['1HASLANE'] == 0].STREET1.value_counts().head(20).plot.bar(title="Top 20 Collision Streets w/o Bike Lane")


# ### Is there a correlation between the number of incidents and the length of the street?

# In[16]:

collisions = df_intersection.STREET1.value_counts()
df_col_len = pd.DataFrame({'ST_NAM':collisions.index, 'COUNT':collisions.values})


# In[17]:

def append_length():
    # This function append the length data to each road
    for idx, row in df_col_len.iterrows():
        if row.ST_NAM in street_length:
            df_col_len.ix[idx,'LENGTH'] = street_length[row.ST_NAM]
        else:
            df_col_len.ix[idx,'LENGTH'] = 0.01


# In[18]:

append_length()


# In[19]:

df_col_len = df_col_len[df_col_len.LENGTH != 0.01]


# In[20]:

df_col_len.head(20)


# In[21]:

df_col_len.to_csv("data/collision_vs_length.csv",index=False)


# In[24]:

# Normalize the Distance Length
# df_col_len = df_col_len[df_col_len['LENGTH'].apply(lambda x: (x - np.mean(x)) / (np.max(x) - np.min(x)))]

# Create x, where x the 'scores' column's values as floats
x = df_col_len['LENGTH'].values.astype(float)

# Create a minimum and maximum processor object
min_max_scaler = preprocessing.MinMaxScaler()

# Create an object to transform the data to fit minmax processor
x_scaled = min_max_scaler.fit_transform(x)

# Run the normalizer on the dataframe
df_normalized = pd.DataFrame(x_scaled, columns=["LENGTH"])


# In[25]:

# Normalize the Count of Collisions
# Create x, where x the 'scores' column's values as floats
x = df_col_len['COUNT'].values.astype(float)

# Create a minimum and maximum processor object
min_max_scaler = preprocessing.MinMaxScaler()

# Create an object to transform the data to fit minmax processor
x_scaled = min_max_scaler.fit_transform(x)

# Run the normalizer on the dataframe
df_cnormalized = pd.DataFrame(x_scaled, columns=["COUNT"])


# In[26]:

df_scatter = pd.concat([df_normalized, df_cnormalized], axis=1)


# In[31]:

df_scatter.info()
df_col_len.info()


# In[29]:

df_scatter.plot.scatter(x='LENGTH', y='COUNT',title="Corelation between Length of Street and Collision")


# In[35]:

df_col_len[['COUNT','LENGTH']].corr(method='pearson')


# In[36]:

# Calculate the correlation coefficient:
df_scatter.corr(method='pearson')


# The correlation between the road length and the collision frequency is quite strong.

# ### What about the collision rates on the bike lanes when there are actucally bike lane exiting on the street?
# For example, there are collision that happened off the bike lanes on a road with bike lane.

# In[37]:

# Create a new DataFrame to count for the whether the narrative mentioned the presence of a bike lane
# 'BLFinal' variable should not be used to identify where bike lanes are in the city
bl = df_collision['BLFinal']
df_BL = pd.DataFrame({'BLFinal':bl.values})
df_BL = pd.concat([df_intersection, df_BL], axis=1)


# In[38]:

df_BL.info()


# In[39]:

df_BL.to_csv("data/collision_on_BikeLane",index=False)


# In[40]:

# Calculate the collision counts off the bike lane
on_BL = df_BL['STREET1'][(df_BL['BLFinal']==1) & (df_BL['1HASLANE']==1)].value_counts()


# In[41]:

# Calculate the collision counts on the bike lane
off_BL = df_BL['STREET1'][(df_BL['BLFinal']==0) & (df_BL['1HASLANE']==1)].value_counts()


# In[43]:

print(on_BL.head(10))
print(off_BL.head(10))


# Apparently we could notice that the collisions happened way often off the bike lane than on the bike lane.

# In[47]:

off_BL.head(20).plot.bar(title="Top 20 Collision OFF Bike Lanes")


# In[45]:

on_BL.head(20).plot.bar(title="Top 20 Collision ON Bike Lanes")


# In[66]:

# Merge two bar plot together for comparison
df_bar = pd.DataFrame({'ST_NAM':off_BL.index, 'COUNT':off_BL.values})
df_temp = pd.DataFrame({'ST_NAM':on_BL.index, 'COUNT':on_BL.values})
df_bar = df_bar.merge(df_temp, left_on=df_bar.ST_NAM, right_on=df_temp.ST_NAM)
df_bar = df_bar.drop(['ST_NAM_y'],axis=1)


# In[73]:

# df_bar.info()
# df_bar.to_csv("data/collision_counts")
df_bar.head(20)


# In[85]:

# df_bar.plot.bar(label='Group 1');
ax = df_bar.plot.bar(x='ST_NAM_x', y='COUNT_x', color='LightBlue', label='Off Bike Lane');
df_bar.plot.bar(x='ST_NAM_x', y='COUNT_y', color='Green', label='On Bike Lane', ax=ax);
ax.set_title("Collision Frequency - On/Off Bike Lane")

