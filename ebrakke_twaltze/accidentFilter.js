db.loadServerScripts();

function flatten(X) {
	db[X].find().forEach(function(x) { db[X].update({_id: x._id}, x.value); });
}

var allCrime = db.ebrakke_twaltze.crimeReports;

dropPerm('accidents');
createPerm('accidents');
allCrime.mapReduce(
	function() {
		var crimeType = 'Motor Vehicle Accident Response';
		if(this.incident_type_description == crimeType && this.location.coordinates[0] !== 0 && this.location.coordinates[1] !== 0){
			emit(this._id, {type: 'accident', latitude: this.location.coordinates[1], longitude: this.location.coordinates[0], report_dt: this.fromdate});
		}
	},              
	function() {},
	{out: 'ebrakke_twaltze.accidents'}
);

flatten('ebrakke_twaltze.accidents');

