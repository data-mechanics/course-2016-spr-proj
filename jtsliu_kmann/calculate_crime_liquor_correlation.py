# Kyle Mann and Jonathan Liu (jtsliu_kmann)
# CS591
#
import datetime
import json
import prov.model
import pymongo
import re
import urllib.request
import uuid
from bson.son import SON

from random import shuffle
from math import sqrt

import scipy.stats


# Open the file for interfacing with DB
exec(open('../pymongo_dm.py').read())


def permute(x):
    shuffled = [xi for xi in x]
    shuffle(shuffled)
    return shuffled

def avg(x): # Average
    return sum(x)/len(x)

def stddev(x): # Standard deviation.
    m = avg(x)
    return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

def cov(x, y): # Covariance.
    return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)

def corr(x, y): # Correlation coefficient.
    if stddev(x)*stddev(y) != 0:
        return cov(x, y)/(stddev(x)*stddev(y))

def p(x, y):
    c0 = corr(x, y)
    corrs = []
    for k in range(0, 2000):
        y_permuted = permute(y)
        corrs.append(corr(x, y_permuted))
    return len([c for c in corrs if abs(c) > c0])/len(corrs)

# Set up the db connection
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('jtsliu_kmann', 'jtsliu_kmann')

#Parse through dataases and put it into a temporary list.
# Josh Mah helped us with this one :)
def getCollection(dbName):
	temp = []
	for elem in repo['jtsliu_kmann.' + dbName].find({}):
		temp.append(elem)
	return temp

zipcode_profile = getCollection('zipcode_profile')

x = [xi["num_crimes"] for xi in zipcode_profile]
y = [yi["liquor_locations"] for yi in zipcode_profile]

print(x,y)

correlation = corr(x, y)
p_val = p(x, y)

print("Done! Corrleation for number of crimes and liquor locations:")
print(correlation, p_val)

print("From library")
print(scipy.stats.pearsonr(x, y))

x = [xi["num_crimes"] for xi in zipcode_profile]
y = [yi["avg_tax_per_sf"] for yi in zipcode_profile]

correlation = corr(x, y)
p_val = p(x, y)

print("Done! Corrleation for number of crimes and avg_tax_per_sf:")
print(correlation, p_val)

print("From library")
print(scipy.stats.pearsonr(x, y))

print("How about the num_crimes compared to the ratio of liquor : total properties")

x = [xi["num_crimes"] for xi in zipcode_profile]
y = [ yi["liquor_locations"] / yi["number_properties"] for yi in zipcode_profile]

print(x,y)

correlation = corr(x, y)
p_val = p(x, y)

print("Done! Corrleation for number of crimes and liquor locations:")
print(correlation, p_val)

print("From library")
print(scipy.stats.pearsonr(x, y))












