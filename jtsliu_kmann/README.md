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


TODO:
Filter out data with invalid locations - done?


Figure out what transformations to do on the data
	crime data - generate a new dataset that contains the crime data with the zip code it occurs in
		Using this, we can aggregate to get the number of crimes per zipcode
	Property and liquor - transform to make a dataset of zipcode with average property assessment and 
					number of liquor licenses
Prov

