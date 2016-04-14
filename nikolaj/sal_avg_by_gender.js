db.loadServerScripts();

var reduceAvgSalaryByGender = function(inColName, outColName) {
    var genderSalaryMapper = function() {
        if (this.value.hasOwnProperty('gender')) {key = this.value.gender;}
        else {key = 'unknown';}
        value = {sal: this.value.sal, count: this.value.count};
        emit(key, value);
    };

    var genderSalaryReducer = function(key, vals) {
        reducedVal = {gender: key, sal: 0, count: 0};
        for (var idx = 0; idx < vals.length; idx++) {
            reducedVal.sal += vals[idx].sal;
            reducedVal.count += vals[idx].count;    
        }
        return reducedVal;
    };

    var genderSalaryFinalizer = function(key, val) {
        val.avg = Math.round(val.sal/val.count);
        return val;
    };

    dropPerm(outColName)
    createPermanent(outColName);    
    db['nikolaj.'.concat(inColName)].mapReduce(
        genderSalaryMapper,
        genderSalaryReducer,
        {
            out: 'nikolaj.'.concat(outColName),
            finalize: genderSalaryFinalizer
        }
    );
};

reduceAvgSalaryByGender('earnings_2013_combined', 'earnings_2013_avg_by_gender');
db.nikolaj.earnings_2013_avg_by_gender.find({});

reduceAvgSalaryByGender('earnings_2014_combined', 'earnings_2014_avg_by_gender');
db.nikolaj.earnings_2014_avg_by_gender.find({});
