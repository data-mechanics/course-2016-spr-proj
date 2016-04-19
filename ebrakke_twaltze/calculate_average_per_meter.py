import prov.model
import datetime
import time
import uuid
import sys
import pymongo
import random
import json
import numpy as np
import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt

exec(open('../pymongo_dm.py').read())
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('ebrakke_twaltze', 'ebrakke_twaltze')

def find_average():
    routes = repo['ebrakke_twaltze.randomDirectionIncidentCount']
    sample_size = routes.count()

    total_potholes_vs_meters = []
    for d in routes.find():
        avg = d['pothole'] / float(d['distance'])
        total_potholes_vs_meters.append(avg)

    total_potholes_vs_meters = np.array(total_potholes_vs_meters)
    mean = np.mean(total_potholes_vs_meters)
    std = np.std(total_potholes_vs_meters)

    plt.hist(total_potholes_vs_meters, 50, normed=1, facecolor='green', alpha=0.75)
    plt.xlabel('Average Potholes per meter')
    plt.ylabel('Number of occurences')
    plt.grid(True)
    plt.savefig('potholes_per_meter.png')


find_average()
