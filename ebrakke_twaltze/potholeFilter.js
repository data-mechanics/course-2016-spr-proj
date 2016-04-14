db.loadServerScripts();

function flatten(X) {
	db[X].find().forEach(function(x) { db[X].update({_id: x._id}, x.value); });
}


var allRequests = db.ebrakke_twaltze.serviceCalls;

dropPerm('potholes');
createPerm('potholes');

allRequests.mapReduce(
	function() {
		var potholeTypes = ['BWSC Pothole', 'Request for Pothole Repair', 'Pothole Repair (Internal)'];
		if(potholeTypes.indexOf(this.type) === -1 && (this.latitude !== 0 && this.longitude !== 0)) {
			emit(this._id, {type: 'pothole', latitude: this.latitude, longitude: this.longitude, report_dt: this.open_dt, status: this.case_status})
		}
	},
	function(k,v) {},
	{out: 'ebrakke_twaltze.potholes'}
);

flatten('ebrakke_twaltze.potholes');
