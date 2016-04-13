db.loadServerScripts();

var input_cols = ["pagerank_result_t_500walk", "pagerank_result_t_only", "pagerank_result_t_500walk_bus"]
var output_col = "nikolaj.pagerank_result"

dropPerm(output_col)
createPerm(output_col)

db["nikolaj." + input_cols[0]].find().forEach(function(outer_st) {
	var st_id = outer_st["id"];
	var combined = {};
	input_cols.forEach(function(col) {
		combined[col] = db['nikolaj.'+ col].findOne({id: st_id});
	});
	db[output_col].insert(combined);
});
