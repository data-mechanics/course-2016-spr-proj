///////////////////////////////////////////////////////
// Name: main.js
// Authors: Daren McCulley and Jasper Burns
// Purpose: Data visualization - Vitality & Composition
// Credit: Peter Cook http://animateddata.co.uk
///////////////////////////////////////////////////////

function getBounds(d, paddingFactor) {
    // Find min and maxes (for the scales)
    paddingFactor = typeof paddingFactor !== 'undefined' ? paddingFactor : 1;

    var keys = _.keys(d[0]), b = {};
    _.each(keys, function(k) {
        b[k] = {};
        _.each(d, function(d) {
            if(isNaN(d[k]))
                return;
            if(b[k].min === undefined || d[k] < b[k].min)
                b[k].min = d[k];
            if(b[k].max === undefined || d[k] > b[k].max)
                b[k].max = d[k];
        });
        b[k].max > 0 ? b[k].max *= paddingFactor : b[k].max /= paddingFactor;
        b[k].min > 0 ? b[k].min /= paddingFactor : b[k].min *= paddingFactor;
    });
    return b;
}

function getCorrelation(xArray, yArray) {
    function sum(m, v) {return m + v;}
    function sumSquares(m, v) {return m + v * v;}
    function filterNaN(m, v, i) {isNaN(v) ? null : m.push(i); return m;}

    // clean the data (because we know that some values are missing)
    var xNaN = _.reduce(xArray, filterNaN , []);
    var yNaN = _.reduce(yArray, filterNaN , []);
    var include = _.intersection(xNaN, yNaN);
    var fX = _.map(include, function(d) {return xArray[d];});
    var fY = _.map(include, function(d) {return yArray[d];});

    var sumX = _.reduce(fX, sum, 0);
    var sumY = _.reduce(fY, sum, 0);
    var sumX2 = _.reduce(fX, sumSquares, 0);
    var sumY2 = _.reduce(fY, sumSquares, 0);
    var sumXY = _.reduce(fX, function(m, v, i) {return m + v * fY[i];}, 0);

    var n = fX.length;
    var ntor = ( ( sumXY ) - ( sumX * sumY / n) );
    var dtorX = sumX2 - ( sumX * sumX / n);
    var dtorY = sumY2 - ( sumY * sumY / n);

    var r = ntor / (Math.sqrt( dtorX * dtorY )); // Pearson ( http://www.stat.wmich.edu/s216/book/node122.html )
    var m = ntor / dtorX; // y = mx + b
    var b = ( sumY - m * sumX ) / n;

    // console.log(r, m, b);
    return {r: r, m: m, b: b};
}

function getMean(array){
    var sum = 0
    for (var i = 0; i < array.length; i++){
        sum += parseFloat(array[i],10);
    }
    return sum/array.length;
}

// from github.com/derickbailey
function standardDeviation(values){
    var avg = getMean(values);

    var squareDiffs = values.map(function(value){
        var diff = value - avg;
        var sqrDiff = diff * diff;
        return sqrDiff;
    });

    var avgSquareDiff = getMean(squareDiffs);

    var stdDev = Math.sqrt(avgSquareDiff);
    return stdDev;
}

d3.json('http://datamechanics.io/data/djmcc_jasper/comp_vit_data.json', function(data) {


    var xAxis = 'percent_com', yAxis = 'vitality';
    var year = '2015';

    
    var years = ["2015"]


    var bounds = getBounds(data, 1);

    // SVG AND D3 STUFF
    var svg = d3.select("#chart")
        .append("svg")
        .attr("width", 1000)
        .attr("height", 640);

    var xScale, yScale;

    svg.append('g')
        .classed('chart', true)
        .attr('transform', 'translate(80, -60)');

    // Build menus
    d3.select('#years')
        .selectAll('li')
        .data(years)
        .enter()
        .append('li')
        .text(function(d) {return d;})
        .style({'font-size': '14px', 'fill': '#000'})
        .classed('selected', function(d) {
            return d === year;             
        })
        .on('click', function(d) {
            year = d;
            updateChart();
            updateMenus();
        });


    d3.select('svg g.chart')
      .append('text')
      .attr({'id': 'zipshadow', 'x': 550, 'y': 600})
      .style({'font-size': '80px', 'font-weight': 'bold', 'fill': '#ddd'});


    d3.select('svg g.chart')
      .append('text')
      .attr({'id': 'correlation', 'x': 18, 'y': 622})
      .style({'font-size': '25px', 'font-weight': 'bold', 'fill': '#bbb'});


    d3.select('svg g.chart')
      .append('line')
      .attr('id', 'bestfit');


    d3.select('svg g.chart')
        .append('text')
        .attr({'id': 'xLabel', 'x': 400, 'y': 670, 'text-anchor': 'middle'})
        .text('Commercial Property Prevalence in Neighborhood');

    d3.select('svg g.chart')
        .append('text')
        .attr('transform', 'translate(-60, 330)rotate(-90)')
        .attr({'id': 'yLabel', 'text-anchor': 'middle'})
        .text('Vitality Score');

    // Render points
    updateScales();
    var color = d3.scale.category10();
    d3.select('svg g.chart')
        .selectAll('circle')
        .data(data)
        .enter()
        .append('circle')
        .attr('cx', function(d) {
            return xScale(d[xAxis]);
        })
        .attr('cy', function(d) {
            return yScale(d[yAxis]);
        })
        .attr('fill', function(d) {return color(d.zipcode);})
        .style('cursor', 'pointer') //ALL THIS TO SHOW ADDRESS AT THE TOP
        .on('mouseover', function(d) {
          d3.select('svg g.chart #zipshadow')
            .text(d.zipcode)
            .transition()
            .style('opacity', 1);
        })
        .on('mouseout', function(d) {
          d3.select('svg g.chart #zipshadow')
            .transition()
            .duration(1000)
            .style('opacity', 0);
        })
        .on("click", function(d) { window.open("https://www.yelp.com/search?find_desc=&find_loc=" + d.zipcode); });;

    updateChart(true);
    updateMenus();

    // Render axes
    d3.select('svg g.chart')
        .append("g")
        .attr('transform', 'translate(0, 630)')
        .attr('id', 'xAxis')
        .call(makeXAxis);

    d3.select('svg g.chart')
        .append("g")
        .attr('id', 'yAxis')
        .attr('transform', 'translate(-10, 0)')
        .call(makeYAxis);

    //// RENDERING FUNCTIONS
    function updateChart(init) {
        updateScales();

        d3.select('svg g.chart')
            .selectAll('circle')
            .transition()
            .duration(500)
            .ease('quad-out')
            .attr('cx', function(d) {return xScale(d[xAxis]);
            })
            .attr('cy', function(d) {return yScale(d[yAxis]);
            })
            .attr('r', 5);

        d3.select('#xAxis')
            .transition()
            .call(makeXAxis);

        d3.select('#yAxis')
            .transition()
            .call(makeYAxis);

        // Update correlation
        var xArray = _.map(data, function(d) {
            if (year == '2015')
                return d[xAxis];
            else if (d.zipcode == year)
                return d[xAxis];
        });
        var yArray = _.map(data, function(d) {
            if (year == '2015')
                return d[yAxis];
            else if (d.zipcode == year)
                return d[yAxis];
        });
        var c = getCorrelation(xArray, yArray);
        var x1 = xScale.domain()[0], y1 = c.m * x1 + c.b;
        var x2 = xScale.domain()[1], y2 = c.m * x2 + c.b;

        // Fade in best fit
        d3.select('#bestfit')
            .style('opacity', 0)
            .attr({'x1': xScale(x1), 'y1': yScale(y1), 'x2': xScale(x2), 'y2': yScale(y2)})
            .transition()
            .duration(1500)
            .style('opacity', 1)

        var corr = Math.round(c.r * 1000) / 1000;
        var r2 =  Math.round(corr * corr * 1000) / 1000
        var xMean = Math.round(getMean(xArray) * 1000) / 1000;
        var yMean = Math.round(getMean(yArray) * 10) / 10;
        var xStdDev = parseInt(standardDeviation(xArray) * 100) / 100.0;
        var yStdDev = parseInt(standardDeviation(yArray) * 100) / 100.0;

        d3.select('#corr')
            .text('R: ' + corr +
                "\nR²:" + r2 +
                "\naX: " + xMean*100 + "% " +
                "\naY: " + yMean +
                "\nsX: " + xStdDev*100 + "% " +
                "\nsY: " + yStdDev);
          //.transition()
          //.style('opacity', 1);
    }

    function updateScales() {
        xScale = d3.scale.linear()
            .domain([bounds[xAxis].min, bounds[xAxis].max])
            .range([20, 780]); // pixel locations

        yScale = d3.scale.linear()
            .domain([bounds[yAxis].min, bounds[yAxis].max])
            .range([600, 100]); // pixel locations
    }

    function makeXAxis(s) {
        s.call(d3.svg.axis()
            .scale(xScale)
            .orient("bottom")
            .tickFormat(d3.format(".0%")));
    }

    function makeYAxis(s) {
        s.call(d3.svg.axis()
            .scale(yScale)
            .orient("left"));
    }

    function updateMenus() {
        d3.select('#years')
            .selectAll('li')
            .classed('selected', function(d) {
                return d === year;
            });
    }

})

