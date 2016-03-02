Needle Dropoff Site Analysis in Boston
==============================

##The Importance of Safe Needle and Syringe Disposal

The safe disposal of needles, syringes, and other sharp medical devices ("sharps") is a public health issue. Americans annually use [over 3 billion needles and sharps] (http://www.fda.gov/MedicalDevices/ProductsandMedicalProcedures/HomeHealthandConsumer/ConsumerProducts/Sharps/) for medical purposes (e.g. for diabetes, allergies, cancer) at home. Workers such as recycling plant workers, waste haulers, and janitors can be injured by [improperly disposed sharps] (http://www.cdc.gov/niosh/topics/bbp/disposal.html). Household members, children, and pets can also be harmed if sharps are disposed of improperly around the home. This can potentially spread infections including [Hepatitis B and C and HIV] (http://www.fda.gov/MedicalDevices/ProductsandMedicalProcedures/HomeHealthandConsumer/ConsumerProducts/Sharps/).

In several states and cities, bans have been imposed on sharp disposal in household waste. Nationwide, there are [certain programs] (http://www.safeneedledisposal.org/general-information/types-of-programs/), such as supervised community collection points, drop boxes, mail-back programs, hazardous waste collection sites, and needle exchange programs, that exist to provide safe means for sharps disposal. Public drop boxes have also been shown to decrease the rate of sharp disposal in [public areas] (http://www.sciencedirect.com/science/article/pii/S0955395909001108) such as parks.

Safe needle and syringe disposal is particularly relevant in Massachusetts, where the disposal of medical sharps in household waste has been illegal [since 2012] (http://news.mit.edu/2012/new-law-on-sharps-disposal-goes-in-effect-july-1-2012). Massachusetts residents use an estimated [2 million needles per week] (http://www.mass.gov/eohhs/docs/dph/environmental/sanitation/faq-needle-disposal.pdf). Furthermore, one of the top category of requests to the City of Boston's [311] (http://www.cityofboston.gov/311/) non-emergency call system is their needle program.

##Project Description

The aim of this project is to determine whether current drop-off sites for needles in the Boston area are located effectively relative to the geographic concentration of needle use/disposal. This project currently uses three sources of data - 311 Service Requests from the City of Boston, longitude/latitude data from OpenCage Geocoder, and a list of drop-off sites from the Boston Public Health Commission (BPHC).

The scripts first pull data associated with the Needle Program from the City of Boston's website and the list of drop-off sites from a BPHC webpage. The two resulting collections of data are a) MongoDB documents with data such as longitude/latitude of the request, start date of request, etc. of calls related to the needle program, and b) MongoDB documents with the location, name, contact information, etc. for each drop-off site. A few other scripts then iterate through the drop-off sites' addresses to retrieve approximate longitude and latitude coordinates from OpenCage Geocoder. The coordinates are merged with the information for the drop-off sites. 

Next, information on hospitals is pulled from the Hospital Locations data set from the City of Boston website. Though hospitals are not generally advertised as public drop-off locations currently, they may also be useful in determining whether they could serve as potential locations in the future.

The location data associated with the requests from the needle program are clustered via k-means using k=6. This value of k was selected by subjective examination of scatterplots generated using scikit-learn's k-means++ method and the matplot library.  

The means output from the k-means clustering will then be used to measure the approximate distance between the means and the longitude/latitude coordinates for the current sites and hospitals.

### Further Considerations and Applications

Data sets that are not currently part of this project/are not publicly available, but might help in driving further applications of this data, include:
- a list of licensed pharmacies in the Boston area, which could serve as potential drop-off sites given their ubiquity
- socioeconomic data by precinct/neighborhood
- data on injuries incurred among workers exposed to sharps or on rate of illegally disposed sharps

Considerations to expand the project include:
- Determining accessibility of current drop-off sites by incorporating road distances instead of Euclidean distances.
- Given a threshold for distance people are willing to travel, determining where drop-off sites would be placed.

More tangential applications:
- Given the data spans multiple years, analyzing data temporally for patterns in requests.
- Analyzing trends of injection drug use, especially opioid injection. [Opioid addiction] (https://www.drugabuse.gov/about-nida/legislative-activities/testimony-to-congress/2016/americas-addiction-to-opioids-heroin-prescription-drug-abuse) and [adolescent prescription opioid misuse] (http://www.ncbi.nlm.nih.gov/pmc/articles/PMC3196821/) are increasing nationwide concerns. Having data such as socioeconomic data, reports of opioid use/overdose by neighborhood, etc. could help in determining whether there is a correlation between the density of concentration of needle use and factors such as socioeconomic status, crime rate, average age, etc. A concern is that publicly available data on opioid addiction in [Massachusetts] (http://www.mass.gov/eohhs/gov/departments/dph/stop-addiction/current-statistics.html) is fairly aggregated and may not be as useful in this context.


## Dependencies/Requirements

### Software/Packages
- Python 3.4
  - [Beautiful Soup 4] (http://www.crummy.com/software/BeautifulSoup/) `bs4`
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
- [City of Boston, 311, Service Requests] (https://data.cityofboston.gov/City-Services/311-Service-Requests/awu8-dc52)
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