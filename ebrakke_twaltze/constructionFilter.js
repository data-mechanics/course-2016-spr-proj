db.loadServerScripts();


function flatten(X) {
	db[X].find().forEach(function(x) { db[X].update({_id: x._id}, x.value); });
}

var allWork = db.ebrakke_twaltze.workZones;


dropPerm('construction');
createPerm('construction');
allWork.mapReduce(
	function() {
		if(this.status !== 'NO START' && this.location && this.location.coordinates[0] !== 0 && this.location.coordinates[1] !== 0) {
			emit(this._id, {type: 'construction', latitude: this.location.coordinates[1], longitude: this.location.coordinates[0], expected_close_date: this.estimatedcompletiondate, num_road_plates: this.roadwayplatesinuse, num_sidewalk_plates:this.sidewalkplatesinuse});
		}
	},
	function() {},
	{out: 'ebrakke_twaltze.construction'}
);

flatten('ebrakke_twaltze.construction');
