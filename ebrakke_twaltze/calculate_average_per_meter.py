import prov.model
import datetime
import time
import uuid
import sys
import dml
import random
import json
import numpy as np
import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('ebrakke_twaltze', 'ebrakke_twaltze')

def find_average():
    routes = repo['ebrakke_twaltze.randomDirectionIncidentCount']
    sample_size = routes.count()

    total_potholes_vs_meters = []
    for d in routes.find():
        avg = d['count'] / (float(d['distance']) / 1000)
        total_potholes_vs_meters.append(avg)

    total_potholes_vs_meters = np.array(total_potholes_vs_meters)
    mean = np.mean(total_potholes_vs_meters)
    std = np.std(total_potholes_vs_meters)

    plt.hist(total_potholes_vs_meters, 50, normed=1, facecolor='green', alpha=0.75)
    plt.xlabel('Average Potholes Per Kilometer')
    plt.ylabel('Number of Occurences')
    plt.grid(True)
    plt.savefig('potholes_per_meter.png')


find_average()
