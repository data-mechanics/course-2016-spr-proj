Project summary

In this project we model the Boston public transportation system as a graph. We investigate different adjacency semantics and explore important graph metrics such as node degree and PageRank. The PageRank algorithm was initially developed to measure the importance of web pages to improve search query result quality. The PageRank of a web page reflects the likelihood that a web-surfer starting at a random webpage and following links at random reaches the webpage in question. By computing the PageRank of a station we expose the likelihood of a random commuter using the transportation network arriving at a particular station. This, in turn, gives indication to the relative importance and centrality of a particular station. By analyzing the station PageRank we hope to expose idiosyncrasies in the overall station layout and get a better understanding of which stations are most central and essential to the network. Modelling the transportation network as a graph further allows us to apply other graph algorithms, such as SimRank to the network. Some concrete questions we want to answer are: which stations in the network are redundant? Which stations are most central? Which routes are most central? Which routes are most similar to each other? How does the inclusion of the bus transportation network affect these centrality scores? How does extending the adjacency semantics by creating edges between stations that are within a short walking distance from each other affect the centrality scores? 

Data sources

http://www.mbtainfo.com/ -- Various bus schedules specifically for the 1, 9, 16, 23, 39, 47, 57, 66, 70, 83, 86, 87, 89, 101, 105, and 116 routes.

https://github.com/mbtaviz/mbtaviz.github.io/tree/master/data -- T lines data both topology and station names.

http://www.mbta.com/uploadedfiles/MBTA_GTFS.zip -- The official MBTA station data.

Transformations

Geo-adjacency. For each station we compute and store all stations within a k-meter radius. This serves as intermediate data for the following PageRank computations but also indicates the vertex degree of each station. 

PageRank of T network only. We computed the PageRank of the graph induced by the green, red, blue, and orange T lines. Two stations are adjacent if there is a direct train connection between them.

PageRank of T network with geo-adjacency. In this model we include edges between directly connected stations but also when stations are within 500 meters of each other (indicating a short-walking distance).

PageRank of T and bus network with geo-adjacency. Same as the previous transformation but also includes the above-mentioned bus routes.

Visualizations

index.html -- PageRank and adjacency over network graph.

map.html -- PageRank of bus and t stations projected onto Google Maps. 

Usage:

Run "python pagerank_pipeline.py" to step through the data generation process.

Open index.html to view visualizations. NOTE: index.html accesses datamechanics.io.

Dependencies:

Python Beautiful Soup: https://www.crummy.com/software/BeautifulSoup/

Pip install: pip install beautifulsoup4

I used this library to scrape mbtainfo.com for bus schedules.
