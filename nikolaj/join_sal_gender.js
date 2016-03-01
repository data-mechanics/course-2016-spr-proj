db.loadServerScripts();

var joinSalaryWithGender = function(salaryData, nameGenderLookup, outputPrefix) {
    // to simplify joining, aggregate on first names first (this guarantees that 
    // each name will be a unique key)
    var reduceSalaryDataByFirstName = function(inColName, outColName) {
        var nameSalaryMapper = function() {
            key = this.name.split(",")[1].split(" ")[0].toLowerCase();    
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

        dropTemp(outColName)
        createTemp(outColName)
        
        db['nikolaj.'.concat(inColName)].mapReduce(
            nameSalaryMapper,
            nameSalaryReducer,
            {
                out: 'nikolaj.'.concat(outColName)
            }
        );   
    };
    // join on first name to get tuples with salary and gender information
    var joinGenderOnName = function(inColName1, inColName2, outColName) {
        var genderMapper = function() {
            emit(this.name.toLowerCase(), {gender: this.gender, all_set: false});
        };

        var nameSalaryMapper = function() {
            value = {sal: this.value.sal, count: this.value.count, all_set: true};
            emit(this._id, value);
        };

        var combineReducer = function(key, vals) {
            reducedVal = {name: key, sal: null, gender: null, count: null};
            all_props_set_counter = 0;
            for (var idx = 0; idx < vals.length; idx++) {
                value = vals[idx];
                if (value.hasOwnProperty("sal")) {
                    reducedVal.sal = value.sal;
                    all_props_set_counter++;
                }
                if (value.hasOwnProperty("count")) {
                    reducedVal.count = value.count;
                    all_props_set_counter++;
                }
                if (value.hasOwnProperty("gender")) {
                    reducedVal.gender = value.gender;
                    all_props_set_counter++;
                }        
            }
            reducedVal.all_set = (all_props_set_counter == 3);
            return reducedVal;
        };

        dropTemp(outColName);
        createTemp(outColName);
        
        db['nikolaj.'.concat(inColName1)].mapReduce(
            nameSalaryMapper,
            combineReducer,
            {
                out: {reduce: 'nikolaj.'.concat(outColName)}
            }
        );
        
        db['nikolaj.'.concat(inColName2)].mapReduce(
            genderMapper,
            combineReducer,
            {
                out: {reduce: 'nikolaj.'.concat(outColName)}
            }
        );
    };    
    reduceSalaryDataByFirstName(salaryData, outputPrefix.concat('reduced_by_name'));
    joinGenderOnName(outputPrefix.concat('reduced_by_name'), nameGenderLookup, outputPrefix.concat('combined'));
    dropTemp(outputPrefix.concat('reduced_by_name'));
};

joinSalaryWithGender('earnings_2013', 'name_gender_lookup', 'earnings_2013_');
// we want to remove name gender lookup tuples that were not used in the reduce step
db.nikolaj.earnings_2013_combined.remove({'value.all_set': false});
db.nikolaj.earnings_2013_combined.find();

joinSalaryWithGender('earnings_2014', 'name_gender_lookup', 'earnings_2014_');
db.nikolaj.earnings_2014_combined.remove({'value.all_set': false});
db.nikolaj.earnings_2014_combined.find();
