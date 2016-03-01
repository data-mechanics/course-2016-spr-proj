#nlouie_thsu8
### Thomas Hsu [thsu@bu.edu](mailto:thsu@bu.edu), Nicholas Louie [nlouie@bu.edu](mailto:nlouie@bu.edu)

#### Boston University Data Mechanics CS591 L1
####course-2016-spr-proj-one


### Running the package

- Create your own auth.json with the format below: 

```json
{
    "services": {
        "cityofbostondataportal": {
            "service": "https://data.cityofboston.gov/",
            "username": username,
            "password": password,
            "token": token (API token for the City of Boston portal)
        }
    }
}  
```

If you wish to obtain the actual datasets, run setup.py
Otherwise, run script.py

### Functionality

- The goal of our `setup.py` is to obtain the data of Boston's Employee Earnings, Crime Incidents, map and reduce the results to obtain for a given year between 2012-2014 average police salary versus crime incidents.

- `setup.py` then outputs these data sets to `nlouie_thsu/data/`

- For the purposes of this project, the data sets have been uploaded to https://data-mechanics.s3.amazonaws.com/nlouie_thsu8/data

- `script.py` Will take the data sets and input them into the MongoDB database. It also generates the provenance data which is also added

### Datasets 

#### Employee Earnings Report
-[2012](https://data.cityofboston.gov/Finance/Employee-Earnings-Report-2012/effb-uspk) 
-[2013](https://data.cityofboston.gov/Finance/Employee-Earnings-Report-2013/54s2-yxpg) 
-[2014](https://data.cityofboston.gov/Finance/Employee-Earnings-Report-2014/4swk-wcg8)

#### Crime Incidents
- [Crime Incidents](https://data.cityofboston.gov/resource/7cdf-6fgx.json). 
- We reduce for years 2012, 2013, 2014.
- We map together the average Police Officer's salary for a year with the crime incidents for that given year. The dataset is then added to our database. 
- We also create provenance data that acts as a log of actions our script has taken from its specific sources. This too is added to our database. 

### Possibilities

- We are interested in the effects of money on the crime in Boston. In particular, for our package how Boston Police Department employee earnings affect crime in Boston.

- With Boston's Employee Earnings dataset, there are many possibilities including creating trends by various departments and attempting to link it with other facts present in Boston (in our case, the police department and amount of crime).

- It may be potentially interesting to use Employee Earnings to track where money is going to the city and flag for any earnings that may seem out of the oridinary. 

