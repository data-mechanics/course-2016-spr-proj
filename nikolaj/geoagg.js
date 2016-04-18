db.loadServerScripts();

var param_id = "geoagg_params";
var dyn_params = db.nikolaj.params.findOne({id: param_id});
var maxDist = Number(dyn_params["maxDistance"]);
var input_cols = dyn_params["input_cols"]
var routeUnion = dyn_params["routeUnion"];
var neighUnion = dyn_params["neighUnion"];

var input_col = "nikolaj.raw_stops"
var output_col = "nikolaj.stops_with_neighs"

dropPerm(input_col);
createPerm(input_col);
input_cols.forEach(function(c) {
    db[c].find().forEach(function(st) { 
        db[input_col].insert(st);
    });
});

db[input_col].ensureIndex({coords:"2dsphere"});

dropPerm(output_col)
createPerm(output_col)
db[input_col].find().forEach(function(x) { 
    x['geo_neighs'] = [];
    x['geo_neigh_routes'] = [];
    db[input_col].find({
        coords: {
            $near: {
                $geometry: x['coords'],
                $maxDistance: maxDist
            }
        }
    }).forEach(function(r) {
        for (i = 0; i < r['routes'].length; i++) {
            x['geo_neigh_routes'].push(r['routes'][i]);
        };
        x['geo_neighs'].push(r['id']);
    });
    db[output_col].insert(x);
});
db[output_col].aggregate(
    [
        { $project: {coords: 1, routes: 1, name: 1, id: 1, idAsArray: ["$id"], 
                     routes: { $setUnion: routeUnion }, 
                     neighs: { $setUnion: neighUnion } } },
        { $project: {coords: 1, routes: 1, name: 1, id: 1, geo_neigh_routes: 1, 
                     neighs: { $setDifference: [ "$neighs", "$idAsArray" ] } } },
        { $out: output_col }
    ]
);
// "There has to be a better way to do the above..." -- me doing pretty much anything in mongodb 
