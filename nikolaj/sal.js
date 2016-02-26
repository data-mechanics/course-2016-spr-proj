db.loadServerScripts();

var computeAvgSalaryByGender = function(salaryData, nameGenderLookup, outputPrefix) {
    var reduceSalaryDataByFirstName = function(inColName, outColName) {
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

        db['nikolaj.'.concat(outColName)].drop();
        createTemp(outColName)
        
        db['nikolaj.'.concat(inColName)].mapReduce(
            nameSalaryMapper,
            nameSalaryReducer,
            {
                out: 'nikolaj.'.concat(outColName)
            }
        );
        print(db['nikolaj.'.concat(outColName)].count());   
    };
    var joinSalaryWithGender = function(inColName1, inColName2, outColName) {
        var genderMapper = function() {
            emit(this.name, {gender: this.gender});
        };

        var nameSalaryMapper = function() {
            value = {sal: this.value.sal, count: this.value.count};
            emit(this._id, value);
        };

        var combineReducer = function(key, vals) {
            reducedVal = {name: key, sal: null, gender: null, count: null};
            for (var idx = 0; idx < vals.length; idx++) {
                value = vals[idx];
                if (value.hasOwnProperty("sal")) {
                    reducedVal.sal = value.sal;
                }
                if (value.hasOwnProperty("count")) {
                    reducedVal.count = value.count;
                }
                if (value.hasOwnProperty("gender")) {
                    reducedVal.gender = value.gender;
                }        
            }
            return reducedVal;
        };

        db['nikolaj.'.concat(outColName)].drop();
        createTemp(outColName);
        
        db['nikolaj.'.concat(inColName1)].mapReduce(
            nameSalaryMapper,
            combineReducer,
            {
                out: {reduce: 'nikolaj.'.concat(outColName)}
            }
        );
        print(db['nikolaj.'.concat(outColName)].count());

        db['nikolaj.'.concat(inColName2)].mapReduce(
            genderMapper,
            combineReducer,
            {
                out: {reduce: 'nikolaj.'.concat(outColName)}
            }
        );
        print(db['nikolaj.'.concat(outColName)].count());
    };
    var reduceAvgSalaryByGender = function(inColName, outColName) {
        var genderSalaryMapper = function() {
            if (this.value.hasOwnProperty('gender')) {key = this.value.gender;}
            else {key = 'unknown';}

            if (this.value.hasOwnProperty('sal')) {s = this.value.sal;}
            else {s = 0;}

            if (this.value.hasOwnProperty('count')) {c = this.value.count;}
            else {c = 0;}

            value = {sal: s, count: c};
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

        db['nikolaj.'.concat(outColName)].drop();
        createPermanent(outColName);
        
        db['nikolaj.'.concat(inColName)].mapReduce(
            genderSalaryMapper,
            genderSalaryReducer,
            {
                out: 'nikolaj.'.concat(outColName),
                finalize: genderSalaryFinalizer
            }
        );
        print(db['nikolaj.'.concat(outColName)].count());
    };
    reduceSalaryDataByFirstName(salaryData, outputPrefix.concat('reduced_by_name'));
    joinSalaryWithGender(outputPrefix.concat('reduced_by_name'), nameGenderLookup, outputPrefix.concat('combined'));
    reduceAvgSalaryByGender(outputPrefix.concat('combined'), outputPrefix.concat('final_result'));
};
computeAvgSalaryByGender('earnings_2014', 'name_gender_lookup', 'earnings_2014_');
db.nikolaj.earnings_2014_final_result.find({});
