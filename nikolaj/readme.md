Project summary

The Boston public transportation system is a complex network of hundreds of stations, routes, and connections. While the T rail network only consists of seven lines there are over 100 bus routes connecting different parts of the city. This complexity makes it hard to pin down shortcomings in the overall station layout or gain concrete insights into its structure. With projects on improving the transportation network by expanding and modifying existing T lines underway it is crucial to create metrics against which to measure the quality of existing (or hypothetical) stations and routes. As of now efforts give a rigorous, computational analysis of the utility of existing stations as well as commuting trends. This type of analysis relies on the pre-existence of rich commuter and station usage data which does not lend itself to preliminary station layout planning when such data is not yet available. We propose to investigate two usage-data-agnostic metrics: structural station importance and route similarity. To this end, we model the transportation network as a graph and apply two popular algorithms from the domain of citation ranking: PageRank and SimRank.

For more info please refer to report.pdf and poster.pdf.

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
