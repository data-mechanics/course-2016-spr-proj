## CS591 Data Mechanics
### Yui Chi Tiffany Lo
### Project One

Files:
- get.py
- clean.py
- add_address.py
- merge.py

Sources:
- Active Food Establishment Licenses: https://data.cityofboston.gov/Permitting/Active-Food-Establishment-Licenses/gb6y-34cq
- Parking Tickets: https://data.cityofboston.gov/dataset/Parking-Tickets/qbxx-ev3s
- Parking Meters: http://bostonopendata.boston.opendata.arcgis.com/datasets/962da9bb739f440ba33e746661921244_9?geometry=-71.309%2C42.317%2C-70.871%2C42.393
- Crime Incidents (Towed Cars): https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports/

Project:
When trying to find somewhere to eat, online reviews offer great feedback for customer service and quality of the food, however parking is a major issue for restaurant patrons that isn't always considered. People do share anecdotal information regarding the parking availability of an eatery in reviews. To look at it based on parking tickets, towed cars, and available parking meters will hopefully provide a clearer picture of which eateries have easily accessible parking and which eateries can benefit from having additional parking meters in their vicinity. A more granular level of looking at the time in which parking tickets or towings occur may also indicate peak times for restaurant patrons to avoid. The eateries can also be aggregated in categories to see if more take out restaurants have issues providing parking to their range of patrons.

Additional Tools:
- Geopy: Geocoding library used to obtain addresses in add_address.py

Transformations:
- Aggregating counts of parking tickets, tows, parking meters, and restaurants based on streets
- Aggregating lists of violations(tickets/tows) based on the day of the week and hour of the day
