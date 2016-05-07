'use strict';

$(function() {
    var map = L.map('map', {
		center: [42.3463844, -71.1043828],
		zoom: 15
	});

	L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token=pk.eyJ1IjoibWFwYm94IiwiYSI6ImNpandmbXliNDBjZWd2M2x6bDk3c2ZtOTkifQ._QA7i5Mpkd_m30IGElHziw', {
		maxZoom: 18,
		attribution: '',
		id: 'mapbox.streets'
	}).addTo(map);

	var routes = new L.FeatureGroup();
	var markers = new L.FeatureGroup();

	$('.routeSearch').on('submit', function() {
		routes.clearLayers();
		markers.clearLayers();

		$.when(getLatLng($('.startingLocation').val())).then(function(s) {
			console.log(s);
			var startLat = s.results[0].geometry.location.lat;
			var startLng = s.results[0].geometry.location.lng;

			$.when(getLatLng($('.endingLocation').val())).then(function(e) {
				console.log(e);
				var endLat = e.results[0].geometry.location.lat;
				var endLng = e.results[0].geometry.location.lng;

				var start = startLat + ',' + startLng;
				var end = endLat + ',' + endLng;

				$.when(getRoutes(start, end)).then(function(routes) {
					var colors = ['red', 'blue', 'green', 'yellow', 'orange'];

					routes.forEach(function(route, index) {
						addRouteToMap(route, colors[index]);
					});
				});
			});
		});

		return false;
	});

	function getLatLng(address) {
		return $.ajax({
			url: 'https://maps.googleapis.com/maps/api/geocode/json',
			data: {
				'address': address
			},
			dataType: 'json',
			success: function(data) {
				return {
					lat: data.results[0].geometry.location.lat,
					lng: data.results[0].geometry.location.lat
				};
			}
		});
	}

	function getRoutes(start, end) {
		return $.ajax({
			url: 'http://server.ebrakke.com:3000',
			data: {
				'start': start,
				'end': end
			},
			dataType: 'json',
			success: function(data) {
				return data;
			}
		});
	}

	function addIncidentToMap(incident) {
		markers.addLayer(L.marker([incident.lat, incident.lng]).addTo(map));
	}

	function addRouteToMap(route, color) {
		var incidents = route.incidents;
		var steps = route.route.legs[0].steps;

		// Place each leg of the route
		var lines = [];
		steps.forEach(function(step) {
			var start = step['start_location'];
			var end = step['end_location'];

			lines.push([[start.lat, start.lng], [end.lat, end.lng]]);
		});
		routes.addLayer(L.multiPolyline(lines, {'color': color}).addTo(map));

		// Place a marker for each pothole
		incidents.forEach(function(incident) {
			addIncidentToMap(incident);
		});
	}
});
