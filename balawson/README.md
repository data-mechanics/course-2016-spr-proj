#Geosocial Data of Boston
##Where are people checking in?

To goal of this project will be to analyze the usefulness of historial social media data as a preditor of human movement. 

*note: throughout, I use first-person plural, but I really mean first-person singular.*
##Datasets

###Brightkite
> Brightkite was a location-based social networking website. Users were able to "check in" at places by using text messaging or one of the mobile applications and they were able to see who is nearby and who has been there before. - Wikipedia

General descriptors of Brightkite (filtered for posts in Boston): 
* Data from April 14, 2008 until October 18, 2010, 917 days
* Number of unqiue users: 1, 383
* Number of posts:       31, 928
* Average number of posts per user: 23

This dataset will be aguemented with the others to help reduce any service specific bais.

###Gowalla
> Gowalla was a location-based social network launched in 2007 and closed in 2012. Users were able to check in at "Spots" in their local vicinity, either through a dedicated mobile application or through the mobile website. - Wikipedia

General descriptors of Gowalla (filtered for posts in Boston): 
* Data from April 23, 2009 until October 22, 2010, 547 days
* Number of unqiue users: 2, 395
* Number of posts:       39, 398 
* Average number of posts per user: 16

This dataset will be aguemented with the others to help reduce any service specific bais.
Legend (TODO: fix crude legend):
* Blue : Brightkite
* Green: Gowalla

![timeline of the datasets above](/balawson/img/postsperdaysmall.png)

###Twitter
This is a curated twitter dataset filtered for geotagged tweets in the Boston area, collected from the public stream. Currently, the only information considered are attributes shared by the Brightkite and Gowalla datasets, geocoordinates and user id. Other information available include the full Twitter API offering, including the actual tweet posted. 

General descriptors of Twitter (filtered for posts in Boston): 
* Data from May 11, 2015 until Feburary 26, 2016, 291 days
* Number of unqiue users: 39, 540 
* Number of posts:       632, 990 
* Average number of posts per user: 16
* (note: I only make 1/6 of the posts available. Let me know if more is needed)

Legend (TODO: fix crude legend):
* Blue : Brightkite
* Green: Gowalla
* Red  : Twitter 


![timeline of the datasets above](/balawson/img/postsperday.png)


###Are these datasets saying the same thing?

#####Posts by the hour (aggregated over the entire data set)
* Colors are the same. 
* y-axis represents the percentage of posts posted during each hour
* 24 hour scale on x-axis
![posts by the hour](/balawson/img/postsbyhour.png)

#####Posts by the hour by location

Brightkite: (still working on labeling which hour is shown)
![heatmap of tweets by the hour](/balawson/notebooks/brightkite.gif)

Gowalla: (still working on labeling which hour is shown)
![heatmap of tweets by the hour](/balawson/notebooks/gowalla.gif)

Twitter: (still working on labeling which hour is shown)
![heatmap of tweets by the hour](/balawson/notebooks/twitter.gif)

#####zoomed into downtown
for some reason I only have them doing one loop. Begins at 0:00 (12am) and ends at 23:00 (the hour preceding midnight). You can click on the image to replay it (open image in new tab).

Brightkite: (still working on labeling which hour is shown)
![heatmap of tweets by the hour](/balawson/notebooks/brightkite_zoom.gif)

Gowalla: (still working on labeling which hour is shown)
![heatmap of tweets by the hour](/balawson/notebooks/gowalla_zoom.gif)

Twitter: (still working on labeling which hour is shown)
![heatmap of tweets by the hour](/balawson/notebooks/twitter_zoom.gif)


####Kolmogorov–Smirnov test
######'a nonparametric test of the equality of continuous probability distributions' - [Wikipedia](https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test)

It is used to determine if two samples came from the same distrutibutions. I use this to compare the three datasets against each other, to see if they are similar. My native approach was to use a two-dimensional test to check if the geocoordinates are similar and a one-dimensional test to check if the psting times are similar. This is an effort to quatify the heatmaps. The code used is hosted [here](http://cs.marlboro.edu/courses/spring2014/jims_tutorials/ahernandez/Apr_25.attachments/scic_stat_tests.py)


####k-means clustering

Clustering this data represents locations that are very attractive. This means there is a large number of posts near these locations. Further clustering methods will incoroporate time of day, number of users, and posts per user. I choose k to be square root of n as a rule of thumb for determing k. This will be improved by one of the metrics described in the [Wikipedia article](https://en.wikipedia.org/wiki/Determining_the_number_of_clusters_in_a_data_set)

####Data Segmentation
We use Open Street Maps to indentify intersections and map social media posts to the closest intersection. Orginal code was created by [Sofia Maria Nikolakaki](https://cs-people.bu.edu/smnikol). New contributions include preserving user ids when associating posts to intersections, for use with mark and recapture and potentially other methods, and a caching mechanism when creating large data strutures. This optimization allows for the scipt to run under 10 minutes (down from >2 hours), thus allowing us to run the script for different datas in a reasonal time.

Here's a visualization of the number of intersections by neighborhood 
![intersectionsbyneightborhood](/balawson/img/numberofintersections.png)
![popular](/balawson/img/popularintersections.png)

####Mark & Recapture
Since social media presence is only a sample of true human movement in the wild, it will be useful to try to estimate what the true population is based on the social media posts collected. Using [Capture-Recapture](http://cs-people.bu.edu/lapets/591/s.php#1121b48a2e1040808d9538ff15ae342f) we essentially treat tweeters, gowalla'ers, and brightkiters as animals that are caught in a trap, given a single day. Then when we check the 'trap' the next day we check to see how many of these users that we've seen before. We will use this estimation on the city level. We do have the true population of Downtown Boston, [16,298](http://archive.boston.com/yourtown/news/downtown/2011/04/census_data_downtown_populatio.html). Using a sample of the twitter data and a waiting period of 5 days, we found an estimate of 2,215. This estimation represents the approximate number of people using twitter which is approx. 7% of the total population in this area. This could be useful in extrapolating into wider domains. 

####Cross-validation
We attempted to use the [web cam](http://www.bu.edu/av/alumni/marsh/image.jpg?1460605284161) to create a truth dataset to cross validate population estimates derived from the sampling methods. The image quality is too small for the default OpenCV person detector. We began data collection to try to use a background subtraction method, but the data but our hard drive space filled up and many of the images were corrupted in the collection process. Currently on hold. Methods of cross-validation will be based on census data. ...still in progress...

####I ask for forgivenss
The XML mess is hideous. Currently working on trying to make it not too much of a pain. There are lots of intermediate files and we are working relying less on these and more of stashing information in mongo.

##Setup
code is for debian-based systems

imagemagick [download](http://www.imagemagick.org/script/binary-releases.php):
```
 sudo apt-get install imagemagick
```

```
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
```
(note: this is build on an Anaconda stack and requirements.txt might be missing a few dependancies, TODO: update)
###To collect data
```
python collect.py
```
###To compare, cluster, and capture
```
python compare.py
python cluster.py
python capture-recapture.py
```
###To generate visualizations of data
```
python data-viz.py
```
(note: this script only generates the gifs. The other pictures in this README were saved from the interactive notebook, TODO add this)
####To interact with data visualization
```
ipython notebook data-viz.ipynb
```
####compile latex
navigate to lposter folder
```
sudo apt-get install latexmk
latexmk  -pvc -pdf report.tex  & #this will compile/display code in the background as you update it
```
