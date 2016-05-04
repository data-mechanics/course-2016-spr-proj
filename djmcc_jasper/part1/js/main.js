//////////////////////////////////////////////////
// Name: main.js
// Authors: Daren McCulley and Jasper Burns
// Purpose: Data visualization - Rent & Assessment
// Credit: Peter Cook http://animateddata.co.uk
//////////////////////////////////////////////////

function getCorrelation(xArray, yArray) {
  
  // helper functions
  function sum(m, v) {return m + v;}
  function sumSquares(m, v) {return m + v * v;}
  function filterNaN(m, v, i) {isNaN(v) ? null : m.push(i); return m;}

  // NaN handling required because selection of a zipcode results in NaNs
  var xNaN    = _.reduce(xArray, filterNaN, []);
  var yNaN    = _.reduce(yArray, filterNaN, []);
  var include = _.intersection(xNaN, yNaN);
  var fX      = _.map(include, function(d) {return xArray[d];});
  var fY      = _.map(include, function(d) {return yArray[d];});
  var sumX    = _.reduce(fX, sum, 0);
  var sumY    = _.reduce(fY, sum, 0);
  var sumX2   = _.reduce(fX, sumSquares, 0);
  var sumY2   = _.reduce(fY, sumSquares, 0);
  var sumXY   = _.reduce(fX, function(m, v, i) {return m + v * fY[i];}, 0);

  var n     = fX.length;
  var ntor  = sumXY - (sumX * sumY / n);
  var dtorX = sumX2 - (sumX * sumX / n);
  var dtorY = sumY2 - (sumY * sumY / n);
 
  // Pearson Correlation Coefficient: http://www.stat.wmich.edu/s216/book/node122.html
  var r = ntor / (Math.sqrt( dtorX * dtorY )); 
  var m = ntor / dtorX; // y = mx + b
  var b = (sumY - m * sumX) / n;

  return {r: r, m: m, b: b};
}

d3.json('http://datamechanics.io/data/djmcc_jasper/assessment_rent_data.json', function(data) {

  var xAxis = 'av_per_unit', yAxis = 'rent', zip = 'Boston';
  var zips = ["Boston", "02215", "02120", "02130", "02135", "02128", "02116", "02134", "02115", "02118"];
  var descriptions = {
    "Boston" : "Boston",
    "02215"  : "Fenway/Kenmore",
    "02120"  : "Mission Hill",
    "02130"  : "Jamaica Plain",
    "02135"  : "Brighton",
    "02128"  : "East Boston",
    "02116"  : "Back Bay",
    "02134"  : "Allston",
    "02115"  : "Longwood/Symphony",
    "02118"  : "South End"
  };
  var ptypeMap = {
    "101"  : "Single Family Dwelling",
    "102"  : "Residential Condo",
    "104"  : "Two Family Dwelling",
    "105"  : "Three Family Dwelling",
    "109"  : "Multiple Buildings/Single Lot",
    "111"  : "Apartments 4-6 Units",
    "112"  : "Apartments 7-30 Units",
    "113"  : "Apartments 31-99 Units",
    "114"  : "Apartments 100+ Units",
    "115"  : "Co-op Apartment"
  };

  // SVG and D3
  var xScale, yScale;

  var svg = d3.select("#chart") // my understanding of '#' is that
    .append("svg")              // it links back to the html like a tag
    .attr("width", 1000)
    .attr("height", 640);
  
  svg.append('g')
    .classed('chart', true)
    .attr('transform', 'translate(80, -60)');

  // build menu
  d3.select('#zipcodes')
    .selectAll('li')
    .data(zips)
    .enter()
    .append('li')
    .text(function(d) {return d;})
    .style({'font-size': '14px', 'fill': '#000'})
    .classed('selected', function(d) {
      return d === zip;
    })
    .on('click', function(d) {
      zip = d;
      updateChart();
      updateMenus();
    });

  d3.select('svg g.chart')
    .append('text')
    .attr({'id': 'address', 'x': 17, 'y': 94})
    .style({'font-size': '16px', 'font-weight': 'bold', 'fill': '#333'});

  d3.select('svg g.chart')
    .append('text')
    .attr({'id': 'correlation', 'x': 17, 'y': 615})
    .style({'font-size': '45px', 'font-weight': 'bold', 'fill': '#bbb'});

  // Best fit line (to appear behind points)
  d3.select('svg g.chart')
    .append('line')
    .attr('id', 'bestfit');

  // Axis labels
  d3.select('svg g.chart')
    .append('text')
    .attr({'id': 'xLabel', 'x': 400, 'y': 670, 'text-anchor': 'middle'})
    .text('Assessed Value per Unit ($)');

  d3.select('svg g.chart')
    .append('text')
    .attr('transform', 'translate(-60, 330)rotate(-90)')
    .attr({'id': 'yLabel', 'text-anchor': 'middle'})
    .text('Rent ($)');

  // Render points
  updateScales();
  var color = d3.scale.category20();
  d3.select('svg g.chart')
    .selectAll('circle')
    .data(data)
    .enter()
    .append("svg:a")
    .attr("xlink:href", function(d) { return 'http://www.cityofboston.gov/assessing/search/?pid=' + d.pid;})
    .attr("target", "_blank")
    .append('circle')
    .attr('cx', function(d) {
      return xScale(d[xAxis]);
    })
    .attr('cy', function(d) {
      return yScale(d[yAxis]);
    })
    .attr('fill', function(d) {return color(d.zipcode);})
    .style('cursor', 'pointer')
    .on('mouseover', function(d) {
      d3.select('svg g.chart #address')
        .text(d.address + ': ($' + d.av_per_unit + ', $' + d.rent + ')' + ' - ' + ptypeMap[d.ptype])
        .transition()
        .style('opacity', 1);
    })
    .on('mouseout', function(d) {
      d3.select('svg g.chart #address')
        .transition()
        .duration(1500)
        .style('opacity', 0);
    })

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
      .attr('cx', function(d) {
        if (zip == 'Boston')
          return xScale(d[xAxis]);
        else if (d.zipcode == zip)
          return xScale(d[xAxis]);
      })
      .attr('cy', function(d) {
        if (zip == 'Boston')
          return yScale(d[yAxis]);
        else if (d.zipcode == zip)
          return yScale(d[yAxis]);
      })
      .attr('r', 3);

    // Update correlation
    var xArray = _.map(data, function(d) {
      if (zip == 'Boston')
        return d[xAxis];
      else if (d.zipcode == zip)
        return d[xAxis];
    });
    var yArray = _.map(data, function(d) {
      if (zip == 'Boston')
        return d[yAxis];
      else if (d.zipcode == zip)
        return d[yAxis];
    });
    var c = getCorrelation(xArray, yArray);
    var x1 = xScale.domain()[0], y1 = c.m * x1 + c.b;
    var x2 = getX2(c), y2 = c.m * x2 + c.b;

    // Fade in best fit
    d3.select('#bestfit')
      .style('opacity', 0)
      .attr({'x1': xScale(x1), 'y1': yScale(y1), 'x2': xScale(x2), 'y2': yScale(y2)}) 
      .transition()
      .duration(1500)
      .style('opacity', 1);

    var corr = Math.round(c.r * 1000) / 1000;

    d3.select('svg g.chart #correlation')
      .html(descriptions[zip] + ': &rho; = ' + corr)
      .transition()
      .style('opacity', 1);
  }

  function getX2(c) {
    if (c.m * 1000000 + c.b > 6000) {
      return (6000 - c.b) / c.m;
    } else {
      return 1000000;
    }
  }

  function updateScales() {
    xScale = d3.scale.linear()
              .domain([100000, 1000000]) // need to alter this to change line end pts
              .range([20, 780]); // pixel locations

    yScale = d3.scale.linear()
              .domain([1000, 6000])
              .range([600, 100]); // pixel locations
  }

  function makeXAxis(s) {
    s.call(d3.svg.axis()
      .scale(xScale)
      .orient("bottom"));
  }

  function makeYAxis(s) {
    s.call(d3.svg.axis()
      .scale(yScale)
      .orient("left"));
  }

  function updateMenus() {
    d3.select('#zipcodes')
      .selectAll('li')
      .classed('selected', function(d) {
        return d === zip;
      });
  }
})

// EOF

