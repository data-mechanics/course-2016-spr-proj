Scoring Age-Friendly Neighborhoods in Boston
==============================

## Introduction

TBD

Projections from the City of Boston's 2014 report on ["Aging in Boston"] (https://www.cityofboston.gov/images_documents/4-14%20UMASS%20Aging%20Report_tcm3-44127.pdf) indicate that by 2030, about 20% of Bostonian residents will be age 60 or older. Boston has demonstrated its commitment to its older residents by [joining] (http://www.cityofboston.gov/news/default.aspx?id=6600) the World Health Organization's Age Friendly Cities Network.

The 2015 United States of Aging Survey, conducted by the National Council on Aging, found that [concerns] (https://www.ncoa.org/news/usoa-survey/2015-results/) among adults aged 60 and older and the professionals who worked with them included physical health, affordable housing, and mental wellbeing.


##Project Description

This project aims to find a way to compute how "age-friendly" a location is, using mostly distance-based metrics that act as a proxy for convenience in accessing important locations for older adults. Factors that are currently to be incorporated into the score include:
1. Distance to nearest MBTA stop  
2. Distance to nearest community center   
3. Distance to nearest hospital  

For a given location, this score is plotted against the HUD and DOT's [Housing Affordability Index] (http://www.locationaffordability.info/about.aspx) for the location's Census Block tract.

Other factors currently not included but potentially be taken in consideration for scoring include:
1. Median property value in that zipcode from Zillow  
2. Median rental value for latest available month for given zipcode from Zillow  
3. Distance to nearest park  
4. Distance to grocery store/supermarket/food markets  
5. Distance to pharmacies  
6. Distance to libraries  
7. Distance to locations of church/faith-based groups  
8. Distance to other art/cultural sites

Sites will either be retrieved from the City of Boston website or from the Yelp API.

Some of the files in this directory pertain to the previous version of this project on a different subject matter and are not used in this version of the project. However they are stored here in the event they are useful.

### Further Considerations and Applications

TBD

This scoring system could be used to randomly select points in a particular neighborhood and calculate an average score for the region. Point selection could occur by using geojson polygon data [here] (http://maptimeboston.github.io/leaflet-intro/neighborhoods.geojson) in combination with a polygon library such as the `shapely` [package] (http://toblerity.org/shapely/shapely.html).

The scores by neighborhood/region could then be compared to the current distribution of adults age 60 and older throughout different neighborhoods in Boston. 


## Dependencies/Requirements

### Software/Packages
- Python 3.4
  - [Beautiful Soup 4] (http://www.crummy.com/software/BeautifulSoup/) `bs4`
  - [lxml] (https://pypi.python.org/pypi/lxml) `lxml`
  - [PyMongo] (https://api.mongodb.org/python/current/) `pymongo`
  - [Prov] (https://pypi.python.org/pypi/prov) `prov`
  - [NumPy] (http://www.numpy.org/) `numpy`
  - [Scikit-learn] (http://scikit-learn.org/stable/) `scikit-learn`
  - [Matplotlib] (http://matplotlib.org/index.html) `matplot-lib`
  - [GeoPy] (https://github.com/geopy/geopy)
- MongoDB 3.2

### Accounts required
- [OpenCage Geocoder] (https://geocoder.opencagedata.com/)
- [Data Boston - City of Boston] (https://data.cityofboston.gov/)
- [Mapquest Developer] (http://www.mapquestapi.com/directions/)
- [Yelp] (https://www.yelp.com/developers/)

## Original Data Sets
- [OpenCage Geocoder] (https://geocoder.opencagedata.com/)
- [FCC Census Block API] (https://www.fcc.gov/general/census-block-conversions-api)
- [MA Zipcodes] (http://www.directoryma.com/MAReferenceDesk/MassachusettsZipCodes.html)
- [MBTA Schedules and Trip Planning Data] (http://www.mbta.com/rider_tools/developers/default.asp?id=21895)
- [City of Boston, Hospital Locations] (https://data.cityofboston.gov/Public-Health/Hospital-Locations/46f7-2snz)


## Instructions

Run `python runallscripts.py` to run all relevant scripts at once.

`auth.json` file must be included in directory to run files. Fields are as follows:

```
{"user": DB_USER,
"service": {
	  	"cityofbostondataportal": {
            "service": "https://data.cityofboston.gov/",
            "username": "USER",
            "password": "PASSWORD",
            "token": "TOKEN",
        },  
        "opencagegeo": {
          "service": "https://geocoder.opencagedata.com/",
          "username": "USER",
          "password": "PASSWORD",
          "key": "TOKEN"
        },
        "yelp": {
          "service": "https://api.yelp.com/v2/",
          "username": "USER",
          "consumer_key": "CONSUMERKEY",
          "consumer_secret": "CONSUMERSECRET",
          "token": "TOKEN",
          "token_secret": "TOKENSECRET"
        },
        "mapquest": {
          "service": "http://www.mapquestapi.com/directions/v2/route",
          "key": "KEY",
          "secret": "SECRET"
        }
}
```

Note that DB_USER should be "jgyou" for the purposes of this project.