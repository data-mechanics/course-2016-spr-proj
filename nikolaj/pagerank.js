db.loadServerScripts();

var param_id = "pagerank_params";
var dyn_params = db.nikolaj.params.findOne({id: param_id});
var input_col = "nikolaj.stops_with_neighs"
var output_col = dyn_params["output_col_name"]

var join = function(attr, X, Y, XY) {
    dropPerm(XY)
    createPerm(XY)
    db[X].find().forEach(function(x) {
        db[Y].find().forEach(function(y) {
            if (x[attr] == y[attr]) {
                for (var attrname in y) { x[attrname] = y[attrname]; }
                db[XY].insert(x);
            }
        });
    });
}

var pagerankMapper = function() {
    var numNeighs = this.to.length;
    for (var i = 0; i < this.to.length; i++) {
        emit( this.to[i], this.rank / numNeighs );
    };
};
var pagerankReducer = function(node, contribs) {
    return Array.sum(contribs);
};
var pagerankFinalizer = function(node, contrib) {
    return contrib * 0.85 + 0.15;
};

dropPerm("nikolaj.neighs")
createPerm("nikolaj.neighs")
dropPerm("nikolaj.ranks")
createPerm("nikolaj.ranks")

db[input_col].find().forEach(function(st) { 
    db.nikolaj.neighs.insert( { node: st.id, id: st.id, to: st.neighs } );
    db.nikolaj.ranks.insert( { node: st.id, id: st.id, rank: 1.0 } );
});

var NUM_ITERATIONS = 40;
// TODO: add convergence check instead of running for fixed number of iterations
for (var i = 0; i < NUM_ITERATIONS; i++) {
    print(i);
    join('node', 'nikolaj.neighs', 'nikolaj.ranks', 'nikolaj.contribs');
    dropTemp("nikolaj.pagerank");
    createTemp("nikolaj.pagerank");
    db.nikolaj.contribs.mapReduce(
        pagerankMapper,
        pagerankReducer,
        { 
            finalize: pagerankFinalizer,
            out: "nikolaj.pagerank"
        }
    );
    dropPerm("nikolaj.ranks");
    db.nikolaj.pagerank.find().forEach(function(r) {
        db.nikolaj.ranks.insert({node: r._id, id: r._id, rank: r.value})
    });
};
join('id', input_col, 'nikolaj.ranks', output_col)
db[output_col].find()
