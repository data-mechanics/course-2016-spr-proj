## CS591 Data Mechanics
### Yui Chi Tiffany Lo
### Project Two

Files:
-get.py: Retrieves the datasets and stores them in MongoDB	
- clean.py: Cleans up the data by standardizing streetname acronyms and removing tickets for violations that are irrelevant to parking concerns
- add_address.py: Adds the street addresses for entries with only longitude and latitude coordinates
- add_zip_long_lat.py: Adds zip codes for entries that only have streetname or longitude and latitude coordinates
- store_location_point.py: Standardizes longitude and latitude data in the form of type Point objects that can be indexed geospatially
- fix_datetime.py: Combines the issue date and issue time field to create a correct datetime field
- merge.py: Merges datasets into aggregate datasets that analyze overall numbers based on zip codes and ticket citations based on day and hour
- output_counts_json.py: Generates JSON file of counts for use by zipcode.html visualization
- output_all_counts_per_zip.py: Generates TXT file containing the number of food establishments, tickets, tow violations, and meters per zip code for use by scatterplots.html
- output_day_time_counts.py: Generates JSON file containing the number of tickets issued per day of the week per hour for use by heatmap.html
- output_violations_per_zip.py: Generates CSV file containing the counts of each of 24 types of ticket violations per zip code for use by violations_per_zip.html
- calc_ticket_fe_corr.py: Calculates p-value for various null hypotheses about relationships between food establishments and tickets/towings
- scoring_areas.py:
- count_nearby.py: Calculates score for restaurants based on number of meters and tickets within a 0.4 mile radius; creates fe_radius collection containing the data, outputs fe_radius.json for use by zipcode.html
- zipcode.html: Map visualization of areas with greatest number of tickets
- zipcode.geojson
- fe_radius.json
- heatmap.html: Heat map visualization of days and times when greatest number of tickets are issued
- daytime_counts_tickets.json
- daytime_counts_towed.json
- scatter_plots.html: Scatter plot visualization of relationships between food establishments and tickets/towings
- violations_per_zip.html: Stacked bar graph visualization of tickets issued in each zip code area broken up by type of violation
- violations_per_zip.csv

Sources:
- Active Food Establishment Licenses: https://data.cityofboston.gov/Permitting/Active-Food-Establishment-Licenses/gb6y-34cq
- Parking Tickets: https://data.cityofboston.gov/dataset/Parking-Tickets/qbxx-ev3s
- Parking Meters: http://bostonopendata.boston.opendata.arcgis.com/datasets/962da9bb739f440ba33e746661921244_9?geometry=-71.309%2C42.317%2C-70.871%2C42.393
- Crime Incidents (Towed Cars): https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports/
- Zip Code shapefiles: https://data.cityofboston.gov/d/af56-j7tb?category=City-Services&view_name=Boston-Neighborhood-Shapefiles

Project:
When trying to find somewhere to eat, online reviews offer great feedback for customer service and quality of the food, however parking is a major issue for restaurant patrons that isn't always considered. People do share anecdotal information regarding the parking availability of an eatery in reviews. To look at it based on parking tickets, towed cars, and available parking meters will hopefully provide a clearer picture of which eateries have easily accessible parking and which eateries can benefit from having additional parking meters in their vicinity. A more granular level of looking at the time in which parking tickets or towings occur may also indicate peak times for restaurant patrons to avoid. The eateries can also be aggregated in categories to see if more take out restaurants have issues providing parking to their range of patrons.

Additionally, understanding what areas are high traffic and incurring many parking violations can provide suggestions to where more parking and clearer signage may be helpful.

Additional Tools:
- Geopy: Geocoding library used to obtain addresses in add_address.py, 

Transformations:
- Aggregating counts of parking tickets, tows, parking meters, and restaurants based on zip codes
- Aggregating lists of violations(tickets/tows) based on the day of the week and hour of the day

Operations:
- Calculating p-values for the null hypothesis a) More food establishments in an area would not lead to more tickets given in an area, b) More food establishments in an area would not lead to more towings happening in an area. 

Given that the p-value for null hypothesis a) was 0.00, there is strong evidence that disproves it, meaning that more food establishments in an area does not lessen the number of tickets given. The disproving of null hypothesis a) does not conflict with the common sense logic that zip codes with more food establishments are likely more busy and therefore have more parking activity that may lead to tickets. 

Then, given that the p-value for null hypothesis b) was 0.63, there is strong evidence that proves it, meaning more food establishments in an area would not lead to more towings happening. The proving of null hypothesis b) encourages further investigation into why cars are being towed and how busy areas with many more food establishments do not see more towings than areas with fewer.

- Scoring areas required that I find the mean and standard deviation for all fafactors in a zip code neighborhood. Then I proceeded to normalize the data on a scale of 0-100 to simplify the scoring process and to compensate for the varying quantities of each factor between zip codes.

Visualizations:
- Coloring zipcodes within Boston based on the quantity of food establishments, parking meters, parking tickets, and towings registered to each area 
- Shading a spectrum to display the number of tickets/towings that occur depending on day and hour within a week
- Scatter plot visualizing the correlation between food establishments to tickets and food establishments to tow violations
- Stacked bar graphs displaying the array of parking violations received within each zipcode area
