db.loadServerScripts();

function flatten(X) {
	db[X].find().forEach(function(x) { db[X].update({_id: x._id}, x.value); });
}

dropPerm('potholes');
createPerm('potholes');
var allRequests = db.ebrakke_twaltze.serviceCalls;

allRequests.mapReduce(
	function() {
		var potholeTypes = ['BWSC Pothole', 'Request for Pothole Repair', 'Pothole Repair (Internal)'];
		if(potholeTypes.indexOf(this.type) === -1 && (this.latitude !== 0 && this.longitude !== 0) && this.case_status !== 'Closed') {
			var key = String(this.latitude)+String(this.longitude)
			emit(key, {type: 'pothole', latitude: this.latitude, longitude: this.longitude, report_dt: this.open_dt, status: this.case_status})
		}
	},
	function(k,v) {
		return v[0];
	},
	{out: 'ebrakke_twaltze.potholes'}
);

flatten('ebrakke_twaltze.potholes');
