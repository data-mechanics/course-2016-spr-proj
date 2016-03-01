function flatten(X) {
		    db[X].find().forEach(function(x) { db[X].update({_id: x._id}, x.value); });
}


var allRequests = db.ebrakke_twaltze.serviceCalls;
var allCrime = db.ebrakke_twaltze.crimeReports;
var allWork = db.ebrakke_twaltze.workZones;

dropPerm('potholes');
createPerm('potholes');
allRequests.mapReduce(
	function() {
		var potholeTypes = ['BWSC Pothole', 'Request for Pothole Repair', 'Pothole Repair (Internal)'];
		if(potholeTypes.indexOf(this.type) === -1 && (this.latitude !== 0 && this.longitude !== 0) {	
			emit(this._id, {type: 'pothole', latitude: this.latitude, longitude: this.longitude, report_dt: this.open_dt, status: this.case_status})
		}
	},
	function(k,v) {},
	{out: 'ebrakke_twaltze.potholes'}
);


dropPerm('vehicleAccidents');
createPerm('vehicleAccidents');
allCrime.mapReduce(
	function() {
		var crimeType = 'Motor Vehicle Accident Response';
		if(this.incident_type_description == crimeType && this.location.coordinates !== [0,0]){
			emit(this._id, {type: 'accident', latitude: this.location.coordinates[1], longitude: this.location.coordinates[0], report_dt: this.fromdate});
		}
	},
	function() {},
	{out: 'ebrakke_twaltze.vehicleAccidents'}
);


dropTemp('roadwayPlates');
createTemp('roadwayPlates');
allWork.mapReduce(
	function() {
		if(this.roadwayplatesinuse !== '0') {
			emit(this._id, {type: 'roadwayPlates', latitude: this.lat, longitude: this.long, expected_close_date: this.estimatedcompletiondate, num_plates: parseInt(this.roadwayplatesinuse)});
		}
	},
	function() {},
	{out: 'ebrakke_twaltze.roadwayPlates'}
);

flatten('ebrakke_twaltze.potholes');
flatten('ebrakke_twaltze.roadwayPlates');
flatten('ebrakke_twaltze.vehicleAccidents');
