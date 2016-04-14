# ebrakke_twaltze

## Highway out of the Danger Zone

Our aim is to analyze road conditions and assign a transportation danger rating to areas of Boston.

### Data sets
1. 311 Service calls
    - Contains reported potholes
2. Public work zones
    - Contains active construction projects and their scale
3. Crime reports
    - Contains car accident reports

### Transformations
1. Basic filtering of data points that are relevant to bike/roadway safety
2. Computations that take various types of data points and transform them into a genertic "danger" level
3. Using k-means, cluster the various danger levels using their latitude and longitude into "danger zones"
    - Sum a cluster's containing danger levels and assign to that cluster's center point

### Further work
The goal is to, given a route through Boston, rate its level of danger. At this point as it pertains to bike safety, but given any data set and a transformation of that data to a generic danger level, this could be expanded to include danger zones relevant to other needs.

### Setup
To run, type `python3 controller.py run`