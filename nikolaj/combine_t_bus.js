db.loadServerScripts();

var param_id = "combine_t_bus";
var dyn_params = db.nikolaj.params.findOne({id: param_id});
var cols = dyn_params["cols_to_combine"]
var output_col = "nikolaj.raw_stops";

dropPerm(output_col);
createPerm(output_col);
cols.forEach(function(c) {
    db[c].find().forEach(function(st) { 
        db[output_col].insert(st);
    });
});
