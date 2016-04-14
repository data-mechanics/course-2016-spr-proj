import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid

import pandas as pd
from pandas import Series, DataFrame

from datetime import datetime

#pd.__version__

# exec(open('../pymongo_dm.py').read())

# Convert all 3 databases into Pandas.DataFrame

# Databas 1: Liquor Licenses
url = 'https://data.cityofboston.gov/resource/hda6-fnsh.json?liccat=CV7AL&city=Boston'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)

# repo.dropPermanent("liquor_license")
# repo.createPermanent("liquor_license")
# repo['enze.liquor_license'].insert_many(r)

df_liquor = pd.read_json(response)

addr = df_liquor['address'].apply(lambda d: d[1:].lower())

# Database 2: Crime Incident Reports
url = 'https://data.cityofboston.gov/resource/7cdf-6fgx.json?'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)

# repo.dropPermanent("crime_report")
# repo.createPermanent("crime_report")
# repo['enze.crime_report'].insert_many(r)

df_crime = pd.read_json(response)

# Database 3: Waze Jam Data
url = 'https://data.cityofboston.gov/resource/yqgx-2ktq.json?city=Boston,%20MA'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)

# repo.dropPermanent("waze_boston")
# repo.createPermanent("waze_boston")
# repo['enze.waze_boston'].insert_many(r)

df_waze = pd.read_json(response)

print(df_liquor.info())
# print(df_liquor.head(3))
print(df_crime.info())
# print(df_crime.head(3))
print(df_waze.info())
# print(df_waze.head(3))

# MapReduce based on street.
# crime_totals = df_crime['streetname'].value_counts()

# df_crime[df_crime['streetname'].apply(lambda d: d.lower()).isin(addr)].streetname.value_counts()

df_crime_mon_total = df_crime[df_crime.day_week == 'Monday']
df_crime_mon_total = df_crime_mon_total[df_crime_mon_total['streetname'].apply(lambda d: d.lower()).isin(addr)].streetname.value_counts()

df_crime_tue_total = df_crime[df_crime.day_week == 'Tuesday']
df_crime_tue_total = df_crime_tue_total[df_crime_tue_total['streetname'].apply(lambda d: d.lower()).isin(addr)].streetname.value_counts()

df_crime_wed_total = df_crime[df_crime.day_week == 'Wednesday']
df_crime_wed_total = df_crime_wed_total[df_crime_wed_total['streetname'].apply(lambda d: d.lower()).isin(addr)].streetname.value_counts()

df_crime_thu_total = df_crime[df_crime.day_week == 'Thursday']
df_crime_thu_total = df_crime_thu_total[df_crime_thu_total['streetname'].apply(lambda d: d.lower()).isin(addr)].streetname.value_counts()

df_crime_fri_total = df_crime[df_crime.day_week == 'Friday']
df_crime_fri_total = df_crime_fri_total[df_crime_fri_total['streetname'].apply(lambda d: d.lower()).isin(addr)].streetname.value_counts()

df_crime_sat_total = df_crime[df_crime.day_week == 'Saturday']
df_crime_sat_total = df_crime_sat_total[df_crime_sat_total['streetname'].apply(lambda d: d.lower()).isin(addr)].streetname.value_counts()

df_crime_sun_total = df_crime[df_crime.day_week == 'Sunday']
df_crime_sun_total = df_crime_sun_total[df_crime_sun_total['streetname'].apply(lambda d: d.lower()).isin(addr)].streetname.value_counts()

crime_mon = df_crime_mon_total.to_frame(name='Monday')
# crime_total.reset_index(level=0, inplace=True)
# crime_total = crime_total.rename(columns = {'index':'Streetname'})

crime_tue = df_crime_tue_total.to_frame(name='Tuesday')
# crime_totals.reset_index(level=0, inplace=True)
# crime_totals = crime_totals.rename(columns = {'index':'Streetname'})

crime_wed = df_crime_wed_total.to_frame(name='Wednesday')

crime_thu = df_crime_thu_total.to_frame(name='Thursday')

crime_fri = df_crime_fri_total.to_frame(name='Friday')

crime_sat = df_crime_sat_total.to_frame(name='Saturday')

crime_sun = df_crime_sun_total.to_frame(name='Sunday')


crime_totals = pd.concat([crime_mon,crime_tue,crime_wed,crime_thu,crime_fri,crime_sat,crime_sun],axis=1)

print(crime_totals)

# out_file = open("crime_totals.json","w")
# json.dump(crime_totals.to_json(),out_file, indent=4)

# jsondata = json.dumps(crime_totals.to_json())
# fd = open('/data/crime_totals.json', 'w')
# fd.write(jsondata)
# fd.close()

# import os

# filename = 'crime_totals.txt'
# dir = os.path.dirname(filename)
# if not os.path.exists(dir):
#     os.makedirs(dir)
# with open(filename, 'w'):
#     json.dump(crime_totals.to_json(),filename)

# with open("crime_totals.json", "w") as output_file:
#     json.dump(crime_totals.to_json(),output_file)

# print(json.dumps(crime_totals.to_json(), indent=4))

crime_totals.to_json()

df_waze_total = df_waze[df_waze['street'].apply(lambda d: d.lower()).isin(addr)].street.value_counts()

waze_total = df_waze_total.to_frame(name='Waze')

print(waze_total)

# out_file = open("waze_total.json","w")
# json.dump(waze_total.to_json(),out_file, indent=4)

waze_total.to_json()

