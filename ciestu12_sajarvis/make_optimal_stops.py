"""
Calculate the optimal stops based on the calculated score with a weighted
k-means algorithm.

Relies on normal_ppl_sec_util, t_branch_info, t_stop_locations, datasets.
"""
import pymongo
import prov.model
import time
import datetime
import uuid
import urllib.request
import json

exec(open('../pymongo_dm.py').read())

teamname = 'ciestu12_sajarvis'
# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate(teamname, teamname)

def dist(p, q):
    (x1,y1) = p
    (x2,y2) = q
    return (x1-x2)**2 + (y1-y2)**2

def plus(args):
    p = [0,0]
    for (x,y) in args:
        p[0] += x
        p[1] += y
    return tuple(p)

def scale(p, c):
    (x,y) = p
    return (x/c, y/c)

def aggregate(s, f):
    keys = {k[0] for k in s}
    return [(key, f([v for k,v in s if k == key])) for key in keys]

def main():
    out_coll = 'green_optimal_stops'
    repo.dropPermanent(out_coll)
    repo.createPermanent(out_coll)

    startTime = datetime.datetime.now()

    # Get all the points and utility measurements.
    stop_weights = repo['{}.{}'.format(teamname, 'normal_ppl_sec_util')].find({})
    lines = repo['{}.{}'.format(teamname, 't_branch_info')].find({})
    stops = repo['{}.{}'.format(teamname, 't_stop_locations')].find({})

    points = [(s['stop_lat'], s['stop_lon']) for s in stops]
    # Some coords are empty, so filter those out, but where did those
    # come from?? Guess this is why we prov.
    points = [(float(lat), float(lon)) for lat,lon in points if lat and lon]

    # We can find which x stops to remove from each branch by finding k x fewer
    # than the number of existing.
    for branch, num_stops in aggregate([(b['line'], 1) for b in lines], sum):
        k = 1
        while k < num_stops:
            print("{} has {} stops currently, let's find {} optimal.".format(
                branch, num_stops, k))
            means = points[:k]

            for i in range(5): # As an alternative to trying for convergence.
                # Since we just select some points as means to start, some will have
                # distance of 0. Select those out.
                point_distances = [(a,(b,dist(a,b))) for a in points for b in means if dist(a,b) != 0]
                point_min_distances = aggregate(point_distances, lambda x: min(x, key=lambda k:k[1]))
                # Average the points for each mean
                sums = aggregate([(b,a) for (a,(b,d)) in point_min_distances], plus)
                counts = aggregate([(b,1) for (a,(b,d)) in point_min_distances], sum)
                means = sorted([scale(b,d) for a,b in sums for c,d in counts if a == c])

            for lat,lon in means:
                elements = {'k':k, 'stop_lat':lat, 'stop_lon':lon, 'line':branch}
                print(elements)
                repo['{}.{}'.format(teamname, out_coll)].insert_one(elements)

            k += 1

    endTime = datetime.datetime.now()

    # Record the provenance document.
    #doc = create_prov(startTime, endTime)
    #repo.record(doc.serialize())
    #print(doc.get_provn())

    repo.logout()


if __name__ == '__main__':
    main()
