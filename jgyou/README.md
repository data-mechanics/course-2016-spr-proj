Needle Dropoff Site Analysis in Boston
==============================

##The Importance of Safe Needle and Syringe Disposal

The safe disposal of needles, syringes, and other sharp medical devices ("sharps") is a public health issue. Americans annually use [over 3 billion needles and sharps] (http://www.fda.gov/MedicalDevices/ProductsandMedicalProcedures/HomeHealthandConsumer/ConsumerProducts/Sharps/) for medical purposes (e.g. for diabetes, allergies, cancer) at home. Workers such as recycling plant workers, waste haulers, and janitors can be injured by [improperly disposed sharps] (http://www.cdc.gov/niosh/topics/bbp/disposal.html). Household members, children, and pets can also be harmed if sharps are disposed of improperly around the home. This can potentially spread infections including [Hepatitis B and C and HIV] (Http://www.fda.gov/MedicalDevices/ProductsandMedicalProcedures/HomeHealthandConsumer/ConsumerProducts/Sharps/).

Nationwide, there are [certain programs] (http://www.safeneedledisposal.org/general-information/types-of-programs/), such as supervised community collection points, mail-back programs, hazardous waste collection sites, and needle exchange programs, that exist to provide safe means for sharps disposal.

In several states and cities, bans have been imposed on sharp disposal in household waste. In Massachusetts specifically, the disposal of medical sharps in household waste has been illegal [since 2012] (http://news.mit.edu/2012/new-law-on-sharps-disposal-goes-in-effect-july-1-2012). Massachusetts residents use an estimated [2 million needles per week] (http://www.mass.gov/eohhs/docs/dph/environmental/sanitation/faq-needle-disposal.pdf). 

##Project Description

This project currently uses three sources of data - 311 Service Requests from the City of Boston, longitude/latitude data from OpenCage Geocoder, and a list of drop-off sites from the Boston Public Health Commission.

Considerations to expand the project include incorporating road distances instead of absolute distances.

Description pending.

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

## Original Sources of Data
- [City of Boston, 311, Service Requests] (https://data.cityofboston.gov/City-Services/311-Service-Requests/awu8-dc52)
- [OpenCage Geocoder] (https://geocoder.opencagedata.com/)
- [Boston Public Health Commission] (http://www.bphc.org/whatwedo/Addiction-Services/services-for-active-users/Pages/Safe-Needle-and-Syringe-Disposal.aspx)
