function getKeys(col) {
	dropTemp('tempKeys');
	var temp = createTemp('tempKeys');
	col.mapReduce(
		function() {
			for (var key in this) {emit(key, null); }
		},
		function(key, stuff) {return null;},
		temp
	)
}
