#nlouie_thsu8
### Thomas Hsu [thsu@bu.edu](mailto:thsu@bu.edu), Nicholas Louie [nlouie@bu.edu](mailto:nlouie@bu.edu)

#### Boston University Data Mechanics CS591 L1
#### course-2016-spr-proj-two

### Functionality

We want to know how streetlights relate to crimes in Boston. We want to know whether or not streetlamp placement has an affect on crime and if there are high-crime areas that may benefit from better lighting.
In order to do this, we compute the distance of each crime (at night) to the closest streetlamp using data provided from [Boston's public datasets](https://data.cityofboston.gov/). 
We think solving this gives insight on how lighting plays a role in the incidences of crime and whether or not certain areas should include more lighting.


### Datasets 

#### Street Lights
- [Street Lights](https://data.cityofboston.gov/Facilities/Streetlight-Locations/7hu5-gg2y).

#### Crime Incidents
- [Crime Incidents](https://data.cityofboston.gov/resource/7cdf-6fgx.json). 

### Transformations
- Convert JSON -> Text
- Feed Text -> Hive/Hadoop
- Get Final Result. 

### Visualizations

- The visualization can be found in `index.html`. 
- The visualization maps every streetlight (using geojson data) in Boston and a sample of the crimes
- This is made with [Leaflet](http://leafletjs.com/) using Lapet's API key...
- Added functionality allows the user to click on the crime to view more details in a popup.

#### StreetLights
![StreetLights](http://puu.sh/ohmgP/184c3ff996.png "StreetLights")
#### StreetLights Zoomed
![StreetLights Zoomed](http://puu.sh/ohmsG/f164a38b1d.png "StreetLights Zoomed")
#### Crime samples
![Crime](http://puu.sh/ohmtz/eb2b04d0a4.png "Crime Samples")
#### Crime Samples Zoomed with Popup
![Crime Zoomed with Popup](https://puu.sh/ohmwp/5ac776b01a.png "Crimed Zoomed with Popup")
