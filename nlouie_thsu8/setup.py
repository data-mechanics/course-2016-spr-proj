'''
Nicholas Louie, Thomas Hsu
nlouie@bu.edu, thsu@bu.edu
nlouie_thsu8
2/27/16
Boston University Department of Computer Science
CS 591 L1 - Data Mechanics Project 1
Andrei Lapets (lapets@bu.edu)
Datamechanics.org
Datamechanics.io
'''


import requests # import sodapy
import json
import dml
import prov.model
import datetime
import uuid
import functools

def map(f, R):
    return [t for (k,v) in R for t in f(k,v)]


def reduce(f, R):
    keys = {k for (k,v) in R}
    return [f(k1, [v for (k2,v) in R if k1 == k2]) for k1 in keys]

# gets the entire dataset.


def json_get_all(addr, limit=50000, offset=0):
    r = requests.get(addr + "&$limit=" + str(limit) + "&$offset=" + str(offset))
    if len(r.json()) < 1000:
        return r.json()
    else:
        j = r.json()
        offset += limit
        while len(r.json()) == limit:
            r = requests.get(addr + "&$limit=" + str(limit) + '&$offset=' + str(offset))
            j = j + r.json()
            offset += limit
            print(len(j))
        return j


def dict_merge(x, y):
    z = x.copy()
    z.update(y)
    return z

# obtains the data for boston police department earnings for year 2012, returns the json data and mapping as a tuple
def get_BPD_earnings_2012():
    # Retrieve json files.
    j = json_get_all('https://data.cityofboston.gov/resource/effb-uspk.json?department=Boston%20Police%20Department')
        # Map the salary of a police man to the year.
    m = map(lambda k, v: [('2012', float(v['total_earnings']))] if v['department'] == 'Boston Police Department' else [], [("key", v) for v in j])
    return (j,m)

def get_BPD_earnings_2013():
    # Retrieve json files.
    j = json_get_all('https://data.cityofboston.gov/resource/54s2-yxpg.json?department=Boston%20Police%20Department')
    # Map the salary of a police man to the year.
    m = map(lambda k, v: [('2013', float(v['total_earnings']))] if v['department'] == 'Boston Police Department' else [], [("key", v) for v in j])
    return (j,m)

def get_BPD_earnings_2014():
    # Retrieve json files.
    j = json_get_all('https://data.cityofboston.gov/resource/4swk-wcg8.json?department_name=Boston%20Police%20Department')
    # Map the salary of a police man to the year. Note: accounts for inconsident field labling.
    m = map(lambda k, v: [('2014', float(v['total_earnings']))] if v['department_name'] == 'Boston Police Department' else [], [("key", v) for v in j])
    return (j,m)

# takes json and outputs to a file name
def output_json(j,s):
    with open(s, 'w') as outfile:
        json.dump(j, outfile, sort_keys = True, indent = 4, ensure_ascii=False)


def main():
    # get data and mapping 2012
    (j12, m) = get_BPD_earnings_2012()
    # output to file
    output_json(j12, 'data/BPDEarnings2012.json')

    # get data and mapping 2013
    (j13,mTemp) = get_BPD_earnings_2013()
    # output to file
    output_json(j13, 'data/BPDEarnings2013.json')

    # update mapping
    m = m + mTemp

    # get data and mapping 2014
    (j14, mTemp) = get_BPD_earnings_2014()
    output_json(j14, 'data/BPDEarnings2014.json')

    # update mapping
    m = m + mTemp

    # Average the salary for each year.
    f = reduce(lambda k,vs: (k, {'avg_salary': sum(vs)/len(vs)}), m)

    print(f)
    output_json(f, 'data/avgEarnings.json')

    # Incidence counts for each year
    f3 = [('2014', 88058), ('2015', 49760), ('2013', 87052), ('2012', 43186)]
    f3 = map(lambda k, v: [(k, {'incidence_count': v})], f3)
    print(f3)
    # Retrieve json files. This takes a longgggg time. Just take my word for it.

    # j = json_get_all("https://data.cityofboston.gov/resource/7cdf-6fgx.json?")
    # # Map an incident to the year with a value of 1.
    # m = map(lambda k, v: [(k, 1)], [(v['year'], v) for v in j])
    # # Reduce by year.
    # f3 = reduce(lambda k, vs: (k, sum(vs)), m)
    output_json(f3,'data/incidentCounts.json')

    ff = reduce(lambda k, vs: dict_merge({'year': k}, functools.reduce(dict_merge, vs)), f + f3)
    print(ff)
    output_json(ff, 'data/avgEarningsIncidents.json')

if __name__ == '__main__':
    main()
