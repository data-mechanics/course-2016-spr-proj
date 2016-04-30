# ebrakke_twaltze

## Project 1 - Highway out of the Danger Zone

Our aim is to analyze road conditions and assign a transportation danger rating to areas of Boston.

### Data sets
1. 311 Service calls
    - Contains reported potholes
2. Public work zones
    - Contains active construction projects and their scale
3. Crime reports
    - Contains car accident reports

### Transformations
1. Basic filtering of data points that are relevant to bike/roadway safety
2. Computations that take various types of data points and transform them into a generic "danger" level
3. Using k-means, cluster the various danger levels using their latitude and longitude into "danger zones"
    - Sum a cluster's containing danger levels and assign to that cluster's center point

### Further work
The goal is to, given a route through Boston, rate its level of danger. At this point as it pertains to bike safety, but given any data set and a transformation of that data to a generic danger level, this could be expanded to include danger zones relevant to other needs.


# Project 2 - Analysis of bike routes in Boston
Our aim of project two was to take some of the data that we gathered about the state of roads in Boston, and allow a user to analyze a route of their choice.  In order to do this, we decided to simplify our dataset and just use the pothole data we gathered from the first project we did.  

In order to approach this, we wanted to come up with an idea of an average number of potholes on a route in Boston.  To gather this information, we picked about 500 different start and end points (latitude, longitude) that fell within the boundaries of the pothole data we had.  With these starting and ending points, we then queries Google maps API to get bicycle directions.  Google maps returned between 1 and 3 different routes for each start and end point supplied.  These routes were stored in a collection as a list of steps.  

Then, for each step, we would query our own collection of potholes in the city and count how many potholes were within .33 meters of this line segment (using a formula to calculate the distances from a point to a line).  Summing up all of these counts gave us the total number of reported potholes on a given route.  

Afterwards, we calculated the average number of potholes per meter on a ride.  Using this number as a baseline, we could then run our pothole finding algorithm on any bike route through Boston and in the end compare its average number of potholes to the average we calculated.  

### Further work
We will be implementing a simple web app for a user to input two location points and then run the suggested google maps routes against our algorithm and our average data set.  This will provide a user with a visual of where on the route all of the potholes are, as well as some indication of how much better this route is than the average route in Boston  

It is also important to note that our baseline dataset is run on completely random data point.  It would be better to store each users starting and ending points into our collection as well and have our average continue to update as people used the app.  Also, our baseline cannot take into account the fact that many people will just stick to the bike path, unless google maps suggested to use the bike path.  This may weight roads that are hardly traveled by bikers unfairly, but again, this could be solved by continuously updating the average.
