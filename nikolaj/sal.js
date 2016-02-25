db.loadServerScripts();

var nameSalaryMapper = function() {
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

var nameSalaryFinalizer = function(key, val) {
    val.avg = Math.round(val.sal/val.count);
    return val;
};

db.nikolaj.reduced_by_name_2014.drop();
createPermanent('reduced_by_name_2014');

db.nikolaj.earnings_2014.mapReduce(
    nameSalaryMapper,
    nameSalaryReducer,
    {
        out: "nikolaj.reduced_by_name_2014"
    }
);
db.nikolaj.reduced_by_name_2014.find({})

// start here
var genderMapper = function() {
    emit(this.name, {gender: this.gender});
};

var nameSalaryMapper = function() {
    value = {sal: this.value.sal};
    emit(this._id, value);
};

var combineReducer = function(key, vals) {
    reducedVal = {name: key, sal: null, gender: null};
    for (var idx = 0; idx < vals.length; idx++) {
        value = vals[idx];
        if(value.hasOwnProperty("sal")) {
            reducedVal.sal = value.sal;
        }
        if(value.hasOwnProperty("gender")) {
            reducedVal.gender = value.gender;
        }        
    }
    return reducedVal;
};

db.nikolaj.combined_2014.drop();
createPermanent('combined_2014');

db.nikolaj.reduced_by_name_2014.mapReduce(
    nameSalaryMapper,
    combineReducer,
    {
        out: {reduce: "nikolaj.combined_2014"}
    }
);
db.nikolaj.combined_2014.find({})

db.nikolaj.name_gender_lookup.mapReduce(
    genderMapper,
    combineReducer,
    {
        out: {reduce: "nikolaj.combined_2014"}
    }
);
db.nikolaj.combined_2014.find({})


