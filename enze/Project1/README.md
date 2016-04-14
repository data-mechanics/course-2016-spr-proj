Enze Yan
enze@bu.edu
CS 591 L1 Spring 2016
Project #1: Data Retrieval, Storage, Provenance, and Transformation

Question: Do most incidents happen near pubs/bars due to alcohol?

What we need:
- Where are the pubs/bars?
- Incidents report with information about what type of incidents and where the incidents happened.
- Traffic condition around pubs/bars.

# Database 1: Liquor Licenses
# URL: https://data.cityofboston.gov/resource/hda6-fnsh

All approved liquor licenses by the Boston Licensing Board
- The dataset was last updated on June 2014
- We first create a filter to get the related license category:
	• located in the greater Boston area (city=Boston)
	• 'CV7AL' is the license code issued to bars/pubs (liccat=CV7AL)
	• We want to make sure that the license is still active (licstatus=Active)
- Important information:
	• Closing (time): is there a particular time interval that has most amount of incidents?
	• Street name & Location: (Latitude, Longitude) -- clustering algorithm with incidents locations

# Database 2: Crime Incident Reports
# URL: https://data.cityofboston.gov/resource/7cdf-6fgx

Crime Incident Reports provided by Boston Police Department:
- The data was documented between July 2012 and August 2015
- Important information:
	• Incident Type Description: Pub Drink, Vandalism, Simple/Aggravated Assault, Robbery ...
	• Day Week = Friday, Saturday, and Sunday
	• Location: (Latitude, Longitude)

# Database 3: Waze Jam Data
# URL: https://data.cityofboston.gov/resource/yqgx-2ktq

Waze Jam data between 2/21/2015 and 3/1/2015. Join with Waze point data set by UUID for detailed location.
Use MapReduce to gather information about each street, specially around bar area.
- Important information:
	• Street Address
	• Start Time
	• Road Type: 1, 2, 6, 7 (no freeway/highway)

### 2 new databases

crime_totals.json sums up (through mapreduce) all the incidents reported located within the same street as pub/bars, separated by day of the week (columns).

waze_totals.json sums up all the traffic jam reported through waze at the same street of pub/bars.

To run:

# Fetch the dataset and build 2 new datasets
python dat_process.py

# Build the provenance file
python initialize.py
