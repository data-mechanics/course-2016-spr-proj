##Purpose
I chose three data sets pulled from the city of boston website. These three data sets include:

1)Location of Hospitals in Boston

2)Waze Jam Data of Streets

3)EMS 911 Dispatch

Databases Created:
Hospitals and if they are located on any recorded Waze Trafficjam streets, and the number of times they show up.
EMS 911 Dispatch time compared to Waze Jam time, and the number of counts that have same hour and minute.


##Future Goals
My goal is to answer the location of nearby hospitals and their relationship to traffic jams, attempting to create
a more efficient way of getting to hospitals. These data sets can be used to eventually identify the time it takes to get to different hospitals, and if it will be faster to go to a farther hospital given a likelihood of traffic jam. 

The second data set can show is if there is a typical time that EMS times are dispatched, and if they are on a similar 
time to traffic jam time. Maybe we can eventually show a correlation between ambulances and traffic jams issues. Another thing we can figure out is the ambulance's relation and how their relationship to traffic jams and the count that have the same hour and minute. 


##Getting First database:

Run proj1.py

This file is currently creating a database, that will have number of intersections between hospitals and jams.
Eventually with more jam data, we will have more intersections.
```
>db.joshmah.intersectionsJamHospitals.find();
```

##Getting Second database:
Run proj1partb.py

This file is currently creating a database that has the number of intersections between time in hours/minutes between
ambulances and traffic jams reported on Waze. 
```
>db.joshmah.intersectionsJamAmbulances.find();
```


##3 different datasets pulled.

Hospitals in Boston - https://data.cityofboston.gov/api/views/46f7-2snz/rows.json

Waze Jam Data of Streets - https://data.cityofboston.gov/api/views/yqgx-2ktq/rows.json

EMS 911 Dispatch Data - https://data.cityofboston.gov/Public-Safety/EMS-911-Dispatch/ak6k-a5up

