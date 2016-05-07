Scoring Age-Friendly Neighborhoods in Boston
==============================

## Introduction

Projections from the City of Boston's 2014 report on ["Aging in Boston"] (https://www.cityofboston.gov/images_documents/4-14%20UMASS%20Aging%20Report_tcm3-44127.pdf) indicate that by 2030, about 20% of Bostonian residents will be age 60 or older. 

Boston has demonstrated its commitment to its older residents by [joining] (http://www.cityofboston.gov/news/default.aspx?id=6600) the World Health Organization's Age Friendly Cities Network. It has also been qualitatively described by the [Milken Institute] (http://successfulaging.milkeninstitute.org/2014/best-cities-for-successful-aging-report-2014.pdf) as being age-friendly because of its high-quality healthcare, its abundance of universities, opportunities for community engagement and cultural vibrancy, and public transportation system. 

However it has been criticized for its high cost of living and expensive healthcare. Some of these factors are important to older adults - for example, the 2015 United States of Aging Survey, conducted by the National Council on Aging, found that [concerns] (https://www.ncoa.org/news/usoa-survey/2015-results/) among adults aged 60 and older and the professionals who worked with them included physical health, affordable housing, and mental wellbeing. Other concerns, brought up by the Aging in Boston report, include greater access to social services and improved transportation access.

Besides examining the aging population, it's important to also consider measures related to the neighborhoods they inhabit. Public and private entities have developed metrics for accessibility that tend to address the needs of the general population. In the UK, the Greater London Authority has developed a series of measures called the Public Transport Accessibility Levels ([PTALs] (http://data.london.gov.uk/dataset/public-transport-accessibility-levels)), explained [here] (https://s3-eu-west-1.amazonaws.com/londondatastore-upload/PTAL-methodology.pdf). Also in the UK, the local government has developed [PERS] (http://content.tfl.gov.uk/what-is-pers.pdf), the Pedestrian Environment Review System, a tool to measure walkability. Commercial services, such as [Walk Score] (https://www.walkscore.com/), also try to quantify walkability.


##Project Description

This project aims to find a way to compute how "age-friendly" a location in Boston is, using mostly distance-based metrics that act as a proxy for convenience in accessing important locations for older adults. Factors that are currently to be incorporated into the score include:  
1. Distance to nearest MBTA stop, weighted by whether wheelchair access is present  
2. Distance to nearest community center  
3. Distance to nearest hospital  

Other factors currently not included but potentially be taken in consideration for scoring include:  
1. Median property value in that zipcode from Zillow  
2. Median rental value for latest available month for given zipcode from Zillow  
3. Distance to nearest [park] (http://www.cityofboston.gov/images_documents/Park%20Directory%20-%20June%202014_tcm3-44633.pdf)  
4. Distance to grocery store/supermarket/food markets  
5. Distance to pharmacies  
6. Distance to [libraries] (http://www.bpl.org/branches/)  
7. Distance to locations of church/faith-based groups  
8. Distance to other art/cultural sites  
9. Availability of affordable or accessible housing from sites such as Mass Affordable Housing  

Sites will mostly be retrieved from the City of Boston website or using the Yelp API. These factors aim to address some of the major concerns brought up by the Aging in Boston report as well as those voiced by other national organizations.

Two visualizations are produced in relation to this project:

a. Some of these locations are mapped using Leaflet in `outputmap.html` to qualitatively show the distribution of sites in different neighborhoods of Boston. The scored locations can also be viewed on the map.  

b. For a given location, its score is plotted against information from the Housing and Urban Development and Department of Transportation's [Housing Affordability Index] (http://www.locationaffordability.info/about.aspx) for the location's Census Block Group. See `retailacesscompare.html`.

Note: for the scripts written during project one on medical sharps disposal in Boston, see the `project_one` directory for more information.

### Further Considerations and Applications

This scoring could be made more sophisticated with measures such as taking into account inpatient bed count at a hospital etc.

This scoring system could be used to randomly select points in a particular neighborhood and calculate an average score for the region. Point selection could occur by using geojson polygon data [here] (http://maptimeboston.github.io/leaflet-intro/neighborhoods.geojson) in combination with a polygon library such as the `shapely` [package] (http://toblerity.org/shapely/shapely.html).

The scores by neighborhood/region could then be compared to the current distribution of adults age 60 and older throughout different neighborhoods in Boston. 

See `poster.pdf` and `agefriendly.pdf` for further information.


#### Current Issues
- Most of the walking distances are calculated using MapQuest's API - these distances currently do not seem to be entirely accurate relative to manually retrieved map results. This needs to be investigated further.
- Needs more thorough method of choosing points as input 
- How to reasonably incorporate different dimensions into a single number, especially when using a limited number of data points
- Geocoding has its limits - for example, postal codes are not always correct as shown [here] (https://help.openstreetmap.org/questions/4244/reverse-geocoding-doesnt-return-postal-code-in-netherlands)
- There also appears to be discrepancies in coordinates found using a geocoder and listed on the MBTA website (in the screenshot below ([alt link] (http://datamechanics.io/data/jgyou/capture.jpg)), coordinates should correspond to State Street, which is downtown, yet map shows up in Cambridge). This is possibly due to limitations of geocoding in general or may be specific to certain data points.

![Screenshot] (http://datamechanics.io/data/jgyou/capture.jpg)


## Dependencies/Requirements

### Software/Packages
- Python 3.4
  - [PyMongo] (https://api.mongodb.org/python/current/) `pymongo`
  - [Prov] (https://pypi.python.org/pypi/prov) `prov`
  - [NumPy] (http://www.numpy.org/) `numpy`
  - [Scikit-learn] (http://scikit-learn.org/stable/) `scikit-learn`
  - [Matplotlib] (http://matplotlib.org/index.html) `matplot-lib`
  - [GeoPy] (https://github.com/geopy/geopy)
  - [Yelp] (https://github.com/Yelp/yelp-python/) 
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
- [City of Boston, Elderly Affairs] (http://www.cityofboston.gov/elderly/center.asp)
- [City of Boston, Elderly Affairs, Programs] (http://www.cityofboston.gov/elderly/agency.asp)
- [Zillow Median Home Value and Rental Indices] (http://www.zillow.com/research/data/)

## Other Websites
- Map markers from Maps Icons Collection [https://mapicons.mapsmarker.com] (https://mapicons.mapsmarker.com)
- Map Neighborhood Geojson Polygons from MapTime Boston [http://maptimeboston.github.io/leaflet-intro/neighborhoods.geojson] (http://maptimeboston.github.io/leaflet-intro/neighborhoods.geojson)


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