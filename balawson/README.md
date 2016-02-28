#Geosocial Data of Boston
##Where are people checking in?

To goal of this project will be to analyze the usefulness of historial social media data as a preditor of human movement. 


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

![timeline of the datasets above](/postsperdaysmall.png)

###Twitter
This is a curated twitter dataset filtered for geotagged tweets in the Boston area, collected from the public stream. Currently, the only information considered are attributes shared by the Brightkite and Gowalla datasets, geocoordinates and user id. Other information available include the full Twitter API offering, including the actual tweet posted. 

General descriptors of Twitter (filtered for posts in Boston): 
* Data from May 11, 2015 until Feburary 26, 2016, 291 days
* Number of unqiue users: 39, 540 
* Number of posts:       632, 990 
* Average number of posts per user: 16

Legend (TODO: fix crude legend):
* Blue : Brightkite
* Green: Gowalla
* Red  : Twitter 


![timeline of the datasets above](/postsperday.png)


###Are these datasets saying the same thing?

#####Posts by the hour (aggregated over the entire data set)
* Colors are the same. 
* y-axis represents the percentage of posts posted during each hour
* 24 hour scale on x-axis
![posts by the hour](/postsbyhour.png)

#####Posts by the hour by location

Brightkite: (still working on labeling which hour is shown)
![heatmap of tweets by the hour](/notebooks/brightkite.gif)

Gowalla: (still working on labeling which hour is shown)
![heatmap of tweets by the hour](/notebooks/gowalla.gif)

Twitter: (still working on labeling which hour is shown)
![heatmap of tweets by the hour](/notebooks/twitter.gif)

