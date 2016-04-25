db.loadServerScripts();

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
