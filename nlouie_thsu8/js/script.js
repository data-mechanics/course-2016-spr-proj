/**
 * Created by nlouie on 4/11/16.
 */
// http://leafletjs.com/examples/geojson.html


function onEachFeature(feature, layer) {
    // does this feature have a property named popupContent?
    if (feature.properties && feature.properties.popupContent) {
        layer.bindPopup(feature.properties.popupContent);
    }
}

var geojsonMarkerOptions = {
    radius: 2,
    fillColor: "#ff7800",
    color: "#000",
    weight: 1,
    opacity: 1,
    fillOpacity: 0.8
};

var geojsonMarkerOptions2 = {
    radius: 3,
    fillColor: "#ff7800",
    color: "#000",
    weight: 1,
    opacity: 1,
    fillOpacity: 0.8
};

var streetlights = L.map('streetlights').setView(L.latLng(42.3562559, -71.1), 10);

L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token=pk.eyJ1IjoibWFwYm94IiwiYSI6ImNpandmbXliNDBjZWd2M2x6bDk3c2ZtOTkifQ._QA7i5Mpkd_m30IGElHziw', {
    maxZoom: 18,
    attribution: '',
    id: 'mapbox.light'
}).addTo(streetlights);


// add street light points

L.geoJson(lights, {
    pointToLayer: function (feature, latlng) {
        return L.circleMarker(latlng, geojsonMarkerOptions);
    },
    onEachFeature: onEachFeature
}).addTo(streetlights);


var crimemap = L.map('crimemap').setView(L.latLng(42.3562559, -71.1), 10);

L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token=pk.eyJ1IjoibWFwYm94IiwiYSI6ImNpandmbXliNDBjZWd2M2x6bDk3c2ZtOTkifQ._QA7i5Mpkd_m30IGElHziw', {
    maxZoom: 18,
    attribution: '',
    id: 'mapbox.light'
}).addTo(crimemap);

for (var i in crimes) {
    var lat = crimes[i].Location.latitude;
    var long = crimes[i].Location.longitude;
    var latlng = L.latLng(lat, long);
    var info = "From Date: " + crimes[i].FROMDATE + ", Description: " + crimes[i].INCIDENT_TYPE_DESCRIPTION;
    var cirMark = L.circleMarker(latlng,geojsonMarkerOptions2).bindPopup(info);
    crimemap.addLayer(cirMark);
}



