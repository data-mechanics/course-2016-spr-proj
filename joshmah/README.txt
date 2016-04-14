##Assignment
I chose three data sets pulled from the city of boston website. These three data sets include:


1)Location of Hospitals in Boston


2)Waze Jam Data of Streets


3)EMS 911 Dispatch

Databases Created:
Hospitals and if they are located on any recorded Waze Trafficjam streets, and the number of times they show up.

EMS 911 Dispatch time compared to Waze Jam time, and the number of counts that have same hour and minute.


##Future Goals

My goal is to answer the location of nearby hospitals and their relationship to traffic jams, attempting to create
a more efficient way of getting to hospitals. This will see if there are certain hospitals that are more likely to cause
traffic jams. I noted that there are some hospitals on the same street, so some of these are research institutes.



The second data set can show is if there is a typical time that EMS times are dispatched, and if they are on a similar 

time to traffic jam time. Maybe we can eventually show a correlation between ambulances and traffic jams issues.

This needs to be approved, because currently, ambulances don't say where they are going, and there is so much traffic in
Boston that it might be in another part of the city. One thing we can use this dataset is to see if ambulances are typical
during traffic jam times.


##Getting First database:


Run proj1.py

This file is currently creating a database, that will have number of intersections between hospitals and jams.

Eventually with more jam data, we will have more intersections.

```
>db.joshmah.intersectionsJamHospitals.find();
```

One thing that I wrote in proj1.py is a dictionary that lists which hospitals are on which street, currently we just have a
street with number of traffic jams that have occured on it.

The provenance for this is recorded in plan.json


##Getting Second database:

Run proj1partb.py

This file is currently creating a database that has the number of intersections between time in hours/minutes between

ambulances and traffic jams reported on Waze. 
```
>db.joshmah.intersectionsJamAmbulances.find();
```

The provenance for this is recorded in plan2.json

##The 3 different datasets and their links


Hospitals in Boston - https://data.cityofboston.gov/api/views/46f7-2snz/rows.json

Waze Jam Data of Streets - https://data.cityofboston.gov/api/views/yqgx-2ktq/rows.json

EMS 911 Dispatch Data - https://data.cityofboston.gov/Public-Safety/EMS-911-Dispatch/ak6k-a5up

