db.loadServerScripts();

db.nikolaj.mr_results.drop();
createTemp('nikolaj.mr_results');

// apparently entries are not unique to a name (or person)
var nameMapper = function() {
    key = this.name.split(",")[1].split(" ")[0];
    value = {sal: parseInt(this.total_earnings), count: 1};
    emit(key, value);
};

var nameSalaryReducer = function(key, vals) {
    reducedVal = {name: key, sal: 0, count: 0};
    for (var idx = 0; idx < vals.length; idx++) {
        reducedVal.sal += vals[idx].sal;
        reducedVal.count += vals[idx].count;
    }
    return reducedVal;
};

var finalizer = function(key, val) {
    val.avg = Math.round(val.sal/val.count);
    return val;
};

db.nikolaj.earnings_2014.mapReduce(
    nameMapper,
    nameSalaryReducer,
    {
        out: "nikolaj.mr_results",
        finalize: finalizer
    }
);
