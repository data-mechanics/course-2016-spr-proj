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

output_col = "dist_routes";
dropPerm(output_col);
createPerm(output_col);

// db.nikolaj.pagerank_result.mapReduce(
//     function() {
//         for (var state in this) {
//             if (state !== "_id") {
//                 this[state].routes.forEach(function(rt) {
//                     emit(rt, "");
//                 }
//             }
//         }
//     },
//     function(k, vals) { return ""; },
//     { out: "nikolaj.".concat(output_col) }
// );

db.nikolaj.pagerank_result.mapReduce(
    function() {
        for (var state in this) {
            if (state !== "_id") {
                this[state].routes.forEach(function(rt) {
                    emit("all", {routes: [rt]}); // need this wrapper for reduce step output
                    emit(state, {routes: [rt]});
                });
            }
        };
    },
    function(k, vals) { 
        var ret = [];
        vals.forEach(function(val) {
            val.routes.forEach(function(v) {
                if (ret.indexOf(v) == -1) {
                    ret.push(v);
                }
            })
        })
        return {routes: ret}; 
    },
    { out: "nikolaj.".concat( output_col ) }
)
