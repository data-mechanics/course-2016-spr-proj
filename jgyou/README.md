Needle Dropoff Site Analysis in Boston
==============================

##The Importance of Safe Needle and Syringe Disposal

The safe disposal of needles, syringes, and other sharp medical devices ("sharps") is a public health issue. Americans annually use [over 3 billion needles and sharps] (http://www.fda.gov/MedicalDevices/ProductsandMedicalProcedures/HomeHealthandConsumer/ConsumerProducts/Sharps/) for medical purposes (e.g. for diabetes, allergies, cancer) at home. Workers such as recycling plant workers, waste haulers, and janitors can be injured by [improperly disposed sharps] (http://www.cdc.gov/niosh/topics/bbp/disposal.html). Household members, children, and pets can also be harmed if sharps are disposed of improperly around the home. This can potentially spread infections including [Hepatitis B and C and HIV] (Http://www.fda.gov/MedicalDevices/ProductsandMedicalProcedures/HomeHealthandConsumer/ConsumerProducts/Sharps/).

Nationwide, there are [certain programs] (http://www.safeneedledisposal.org/general-information/types-of-programs/), such as supervised community collection points, mail-back programs, hazardous waste collection sites, and needle exchange programs, that exist to provide safe means for sharps disposal. In several states and cities, bans have been imposed on sharp disposal in household waste.

Safe needle and syringe disposal is particularly relevant in Massachusetts, where the disposal of medical sharps in household waste has been illegal [since 2012] (http://news.mit.edu/2012/new-law-on-sharps-disposal-goes-in-effect-july-1-2012). Massachusetts residents use an estimated [2 million needles per week] (http://www.mass.gov/eohhs/docs/dph/environmental/sanitation/faq-needle-disposal.pdf). Furthermore, one of the top category of requests to the City of Boston's [311] (http://www.cityofboston.gov/311/) non-emergency call system is their needle program.

##Project Description

The aim of this project is to determine whether current drop-off sites for needles in the Boston area are located effectively, and to determine where a hypothetical additional site might be located. This project currently uses three sources of data - 311 Service Requests from the City of Boston, longitude/latitude data from OpenCage Geocoder, and a list of drop-off sites from the Boston Public Health Commission (BPHC).

The scripts first pull data associated with the Needle Program from the City of Boston's website and the list of drop-off sites from a BPHC webpage. Unlike the constantly updated 311 Requests data, the list of drop-off sites would not need to be retrieved too often. Nonetheless, a script was created to ease data transfer. There is also flexibility for the BPHC webpage data to be substituted with other departmental listings. The two resulting collections of data are a) MongoDB documents with data such as longitude/latitude of the request, start date of request, etc. of calls related to the needle program, and b) MongoDB documents with the location, name, contact information, etc. for each drop-off site.

Iterating through the drop-off sites' addresses, approximate longitude and latitude coordinates are then pulled from OpenCage Geocoder.

The location data associated with the requests from the needle program are clustered with the k-means algorithm using various values of k. The output from the k-means clustering will then be compared to the longitude/latitude coordinates for the current sites.

Considerations to expand the project include:
- incorporating road distances instead of Euclidean distances
- analyzing data temporally for patterns in requests

## Dependencies/Requirements

### Software/Packages
- Python 3.4
  - [Beautiful Soup 4] (http://www.crummy.com/software/BeautifulSoup/) `bs4`
  - [PyMongo] (https://api.mongodb.org/python/current/) `pymongo`
  - [Prov] (https://pypi.python.org/pypi/prov) `prov`
- MongoDB 3.2

### Accounts required
- [OpenCage Geocoder] (https://geocoder.opencagedata.com/)
- [Data Boston - City of Boston] (https://data.cityofboston.gov/)

## Original Data Sets
- [City of Boston, 311, Service Requests] (https://data.cityofboston.gov/City-Services/311-Service-Requests/awu8-dc52)
- [OpenCage Geocoder] (https://geocoder.opencagedata.com/)
- [Boston Public Health Commission] (http://www.bphc.org/whatwedo/Addiction-Services/services-for-active-users/Pages/Safe-Needle-and-Syringe-Disposal.aspx)

## Running scripts

Description pending.