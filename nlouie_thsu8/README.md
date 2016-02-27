# Boston University Data Mechanics CS591 L1
# course-2016-spr-proj-one

## Thomas Hsu (thsu@bu.edu), Nicholas Louie (nlouie@bu.edu)
Date: 2/27/16

### Functionality

The goal of our ** script.py ** is to obtain the data of Boston's Employee Earnings. We take the years [2012](https://data.cityofboston.gov/Finance/Employee-Earnings-Report-2012/effb-uspk), [2013](https://data.cityofboston.gov/Finance/Employee-Earnings-Report-2013/54s2-yxpg), and [2014](https://data.cityofboston.gov/Finance/Employee-Earnings-Report-2014/4swk-wcg8).

We also obtain the dataset for [Crime Incidents](https://data.cityofboston.gov/resource/7cdf-6fgx.json). We extract for years 2012, 2013, 2014.

We map together the average Police Officer's salary for a yearwith the crime incidents for that given year. The dataset is then added to our database. 

We also create provenance data that acts as a log of actions our script has taken from its specific sources. This too is added to our database. 

### Possibilities

With Boston's Employee earings dataset, there are many possibilities including creating trends by various departments and attempting to link it with other facts present in Boston (in our case, the police department and amount of crime).

It may be potentially interesting to use Employee Earnings to track where money is going to the city and flag for any earnings that may seem out of the oridinary. 

