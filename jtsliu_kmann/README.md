Narrative
---------

Since our first project our goal has changed slightly. Our current goal is to classify a zipcode.
More specifically, we wish to allow a user to specify some constraits and have them recieve a zipcode
that satisfies those constraints. Something like this would include: "I want the safest neighborhood
with the lowest tax rates, lowest crime rates, with 2 schools, and 1 hospital". We could still come up
with a way to have a specific metric rating (1 - 10 per say) of a zipcode. That being said, we currently
support queries of the above type to an extent (see the solutions sections for more details and desires).
In order to try and solve the problem we have done the following. We have selected some data from
the city of Boston (see source sets for details); these include, property information, crime rates, liquor
license, public schools, and hospitals. We next combined data sets to get a "profile" for a zipcode.
We attempted to correlate crimes to alcohol by the street address and proximity (approx. walking distance)
between the crime and liquor source. Additionally, we calculate the average tax per square foot in USD.
From this we also see how many hospitals and schools are in each zipcode. Using all this, we can create
a profile for the neighborhood and begin to do some optimization and satisfaction problems. Additionally,
we wanted to investigate if there is any correlation between liquor sales availability 
and crime - particularly crime involving alcohol.

Source Datasets
----------------

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
from the census. We calculate the average tax per square foot in properties.

4. Public Schools: https://data.cityofboston.gov/resource/e29s-ympv

This is used to rate a zipcode - how many schools does it have?

5. Hospital Locations: https://data.cityofboston.gov/resource/46f7-2snz

Also used to rate a zipcode - how many hospitals does it have?

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
$ python3 fetch_public_school_data.py
$ python3 fetch_hospital_data.py

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

Additional transformations (implemented in project 2)
------------------------------------------------------

$ python3 create_crime_near_alcohol.py

This was inspired by Enze's dataset; however, when we went to use it, we noticed a 
few things we wanted to change, so we drew from his idea and created our own dataset.
Using this dataset we are approximating how many crimes occured near a liqour sale.
We can then aggregate this for a count

$ python3 create_zipcode_profile.py

This creates the actual profile from the zipcodes. This is mostly aggregations of all the above
datasets. We count how many alcohol associated crimes occured, the tax rate, the number of schools,
and the number of hospitals. We then try to get useful information from this.

Problem Solutions (project 2)
------------------------------

First we wanted to see if there was a correlation between various data elements in a zipcode.
To see our results run:

$ python3 calculate_crime_liquor_correlation.py

We believe this data is slightly skewed because of a lack of judicious filtering of crime.


Now we wanted to solve a problem - the one we originally described. Based on some user input, what is the
optimal zipcode. NOTE: we still want to refine this to be able to maximize/minimize in multiple dimensions

$ python3 solve_optimal_zipcode.py

Limitations and problems
-------------------------

Currently we cannot do sophisticated results for our optimal zipcode (outside of what you see
when you run it). Ideally we would like to turn this into a more complicated optimization problem.

The zipcode as mentioned above is a pretty large area, maybe we should explore smaller subsets.

We should filter out more crimes that we associate with liquor licenses.

TODO:
-----

Implement and refine the visualizations
Refactor and comment the code more

