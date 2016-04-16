db.loadServerScripts();

var param_id = "geoagg";
var dyn_params = db.nikolaj.params.findOne({id: param_id});
var maxDist = Number(dyn_params["maxDistance"]);
var routeUnion = dyn_params["routeUnion"];
var neighUnion = dyn_params["neighUnion"];

var input_col = "nikolaj.raw_stops"
var output_col = "nikolaj.stops_with_neighs"

db[input_col].ensureIndex({coords:"2dsphere"});

var XY = output_col;
var X = input_col;
var Y = input_col;

dropPerm(XY)
createPerm(XY)
db[X].find().forEach(function(x) { 
    x['geo_neighs'] = [];
    x['geo_neigh_routes'] = [];
    db[Y].find({
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
    db[XY].insert(x);
});
// this is dynamic
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
