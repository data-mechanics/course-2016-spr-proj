Our current goal is to evaluate neighborhoods in Boston, and determine if a
neighborhood has become more "dangerous" over time. Another possibility we hope
to keep in mind is to be able to rate an area or all areas based on some metrics, and
then if there is some business/building you are looking to establish, you can see
the most prospectable area for this building. Currently we are looking at the following
data to try and solve this problem:

1. Crime data from city of Boston: https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports/7cdf-6fgx

This dataset was chosen because we believe it represents a pretty strong correlation
to the dangerousness of a neighborhood - high violent crime is probably a dangerous
area.

2. Liquor license data from city of Boston: https://data.cityofboston.gov/dataset/Liquor-Licenses/hda6-fnsh

This dataset is a bit of an experiment. Perhaps it is the case that in areas with easily
available alcohol, there is high crime rate. In particular, violent crime related to
alcohol - this would be a cause of a neighborhood becoming more dangerous potentially.

3. Property Assessment: https://data.cityofboston.gov/resource/qz7u-kb7x

This dataset was chosen to see if we could learn anything about the value of an area based on the assessment
from the census. Ideally this, combined with some other data such as poverty, crime, or homelessness (can you measure that?),
would yield interesting metrics on the gentrification of an area.

Currently our transformations classify all of the data by zipcode, which is a fairly large geospacial area.
Perhaps we could narrow it to locations in the future, but currently, we can try to classify zipcodes based on
some interesting information.

Running data retrieval
----------------------

Note: please run the fetch_liquor_data file first for fresh runs, as this generates the plan.json file
again.

To run files:

$ python3 fetch_liquor_data.py
$ python3 fetch_property_assessment.py
$ python3 fetch_crime_data.py

Dependencies: These files do not have any dependencies outside the expected ones (prov, pymongo, json, etc.)

Transformations
---------------

We run the transformations as follows:

$ python3 create_zipcode_with_liquor_and_property_value.py

This file uses the liquor data and property data we collected as resources. From them, we derive a new data set that
holds information about a zip code: how much the average tax per square foot is and how many liquor locations there are.

The next transformation does have some problems. Mainly we are trying to work around reaching our request limit
for geolocation services. We want to reverse geolocate the zipcode of a crime, so we can use the same metric we did above to 
evaluate a zipcode. Currently we do not have the entirety of the data processed, but plan on implementing a different method

$ python3 reverse_geocode.py

Dependencies: Same as previous scripts with the addition of the geopy module (pip3 install geopy)
We use this library for obtaining zipcodes

IMPORTANT: when running this script for the first time, uncomment lines 34 and 35 to 
properly add the data to the repo; however, due to the current limitations, we comment this
out since subsequent runs result in dropping the database with no way to regenerate the data because
we lack the requests in our api key.

Notes: we would like to construct a full data set for the latter script, and hope to do so soon.
When generating the provenance for the transformations, any dataset we used for the transformation
was added as a resource that the new dataset was derived from

