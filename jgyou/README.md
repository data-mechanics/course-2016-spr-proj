A Metric for Age-Friendly/Accessible Neighborhoods in Boston
==============================

## Introduction

TBD

Projections from the City of Boston's 2014 report on ["Aging in Boston"] (https://www.cityofboston.gov/images_documents/4-14%20UMASS%20Aging%20Report_tcm3-44127.pdf) indicate that by 2030, about 20% of Bostonian residents will be age 60 or older. Boston has demonstrated its commitment to its older residents by [joining] (http://www.cityofboston.gov/news/default.aspx?id=6600) the World Health Organization's Age Friendly Cities Network.

The 2015 United States of Aging Survey, conducted by the National Council on Aging, found that [concerns] (https://www.ncoa.org/news/usoa-survey/2015-results/) among adults aged 60 and older and the professionals who worked with them included physical health, affordable housing, and mental wellbeing.


##Project Description

Factors to potentially be taken in consideration for scoring include:
1. Nearest MBTA bus/subway/other stop  
2. Community centers  
3. General neighborhood affordability, as measured by median property value in that zipcode  
4. Distance to nearest hospital  
5. Distance to nearest park  
6. Grocery store/supermarket/food markets  
7. Pharmacies  
8. Libraries  
9. Locations of church/faith-based groups  
10. Other art/cultural sites

### Further Considerations and Applications

TBD

This scoring system could be used to randomly select points in a particular neighborhood and calculate an average score for the region. This could then be compared to the current distribution of adults age 60 and older throughout different neighborhoods in Boston.


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
- MongoDB 3.2

### Accounts required
- [OpenCage Geocoder] (https://geocoder.opencagedata.com/)
- [Data Boston - City of Boston] (https://data.cityofboston.gov/)

## Original Data Sets
- [City of Boston, Hospital Locations] (https://data.cityofboston.gov/Public-Health/Hospital-Locations/46f7-2snz)
- [OpenCage Geocoder] (https://geocoder.opencagedata.com/)
- [Boston Public Health Commission] (http://www.bphc.org/whatwedo/Addiction-Services/services-for-active-users/Pages/Safe-Needle-and-Syringe-Disposal.aspx)

## Instructions

Run `python runscripts.py` to run all relevant scripts at once.

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
        }
}
```

Note that DB_USER should be "jgyou" for the purposes of this project.