import matplotlib.pyplot as plt
import random
import pandas as pd
import math
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np

from bson import ObjectId

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)

agg = [{
        "United States": {},\
        "Massachusetts": {},\
        "Boston": {},\
        "Allston/Brighton": {},\
        "Back Bay": {},\
        "Central": {}, \
        "Charlestown": {}, \
        "East Boston": {}, \
        "Fenway/Kenmore": {}, \
        "Harbor Islands": {}, \
        "Hyde Park": {}, \
        "Jamaica Plain": {}, \
        "Mattapan": {}, \
        "North Dorchester": {},\
        "Roslindale": {}, \
        "Roxbury": {}, \
        "South Boston": {}, \
        "South Dorchester": {}, \
        "South End": {}, \
        "West Roxbury": {}
        }]
areas = [
        "United States",\
        "Massachusetts",\
        "Boston",\
        "Allston/Brighton",\
        "Back Bay",\
        "Central", \
        "Charlestown", \
        "East Boston", \
        "Fenway/Kenmore", \
        "Harbor Islands", \
        "Hyde Park", \
        "Jamaica Plain", \
        "Mattapan", \
        "North Dorchester",\
        "Roslindale", \
        "Roxbury", \
        "South Boston", \
        "South Dorchester", \
        "South End", \
        "West Roxbury"
        ]
def createAgePie(ages, area):
    # pie = {}
    labels = []
    sizes = []
    colors = ['gold', 'yellowgreen', 'lightcoral', 'lightskyblue', 'r', 'm']
    median = 0
    print(ages)
    for index, key in enumerate(ages):
        if "-" in key:
            # pie[key] = age[key]
            labels.append(key)
            sizes.append(ages[key])
        elif "+" in key:
            # pie[key] = age[key]
            labels.append(key)
            sizes.append(ages[key])
        elif "Under" in key:
            # pie[key] = age[key]
            labels.append(key)
            sizes.append(ages[key])
        elif "Median" in key:
            median = ages[key]
    # plt.figure(index)
    plt.pie(sizes, labels = labels, autopct='%1.1f%%', shadow = True, startangle = 90)
    plt.axis('equal')
    plt.title(area + ' Age Distribution', fontsize = 14, fontweight = 'bold', y = 1.05)
    plt.text(-1.5, 0.85 , "Median Age: " + str(median), bbox=dict(facecolor='white', alpha=0.5), fontsize=14)  
    if "/" in area:
        area = area.replace("/", "-")
    plt.savefig('visual/' + area + '_Age_Distribution')
    plt.clf()
    # print(json.dumps(pie, indent = 4))
    print(labels)
    print(sizes)

def createMedAge(data):
    y = []
    xTicks = []
    for key in areas:
        xTicks.append(key)
        y.append(data[key]['Age']['Median Age'])
    x = list(range(1, len(xTicks) + 1))
    print(x)
    print(xTicks)
    print(y)
    plt.xticks(x, xTicks, rotation = 30, ha='right')
    plt.tight_layout()
    plt.scatter(x,y)
    plt.title("Median Age", fontsize = 14, fontweight = 'bold')
    plt.savefig('visual/' + 'Median Age')
    plt.clf()

def createEduPie(x, area):
    # pie = {}
    labels = []
    sizes = []
    colors = ['gold', 'yellowgreen', 'lightcoral', 'lightskyblue']
    median = 0
    print(x)
    for index, key in enumerate(x):
        if "Bachelor" in key:
            labels.append(key)
            sizes.append(x[key])
        elif "graduate" in key:
            labels.append(key)
            sizes.append(x[key])
        elif "college" in key:
            labels.append(key)
            sizes.append(x[key])
    plt.pie(sizes, labels = labels, autopct='%1.1f%%', shadow = True, startangle = 90)
    plt.axis('equal')
    plt.title(area + ' Education Distribution', fontsize = 14, fontweight = 'bold', y = 1.05)
    if "/" in area:
        area = area.replace("/", "-")
    plt.savefig('visual/' + area + '_Education_Distribution')
    plt.clf()
    # print(json.dumps(pie, indent = 4))
    print(labels)
    print(sizes)
    
def createMedAge(data):
    y = []
    xTicks = []
    yTicks = ['', 'Less than high \nschool graduate', \
            'High school graduate', \
            'Some college or \nassociate\'s degree', \
            'Bachelor\'s degree \nor higher']
    for area in areas:
        xTicks.append(area)
        avg = 0
        total = 0
        for level in data[area]['Education']:
            if 'Less' in level:
                avg += 1*data[area]['Education'][level]
                total += data[area]['Education'][level]
            elif 'High' in level:
                avg += 2*data[area]['Education'][level]
                total += data[area]['Education'][level]
            elif 'Some' in level:
                avg += 3*data[area]['Education'][level]
                total += data[area]['Education'][level]
            elif 'Bachelor' in level:
                avg += 4*data[area]['Education'][level]
                total += data[area]['Education'][level]           
        y.append(avg/total)
    x = list(range(1, len(xTicks) + 1))
    print(x)
    print(xTicks)
    print(y)
    print(list(range(0, 5, 1)))
    plt.xticks(x, xTicks, rotation = 60, ha='right')
    plt.yticks(np.arange(0, 5, 1.0), yTicks)
    plt.ylim(0,4,1)
    plt.tight_layout()
    plt.scatter(x,y)
    plt.title("Average Education", fontsize = 14, fontweight = 'bold')
    plt.savefig('visual/' + 'Average Education')
    plt.clf()

_file = open('data/Boston_Demographics.json')
data = json.load(_file)[0]
empty = agg[0]
for area in areas:
    ages = data[area]['Age']
    print(json.dumps(ages, indent = 4))
    createAgePie(ages, area)
createMedAge(data)

for area in areas:
    education = data[area]['Education']
    print(json.dumps(education, indent = 4))
    createEduPie(education, area)
createMedAge(data)
