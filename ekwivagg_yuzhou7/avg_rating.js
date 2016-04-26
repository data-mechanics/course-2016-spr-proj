$(document).ready(function () {
  //var rating;
  //d3.json("http://datamechanics.io/data/ekwivagg_yuzhou7/avg.json", function(error, data) {
    //console.log(data)
    //rating = data;
  //});

  var bubbleChart = new d3.svg.BubbleChart({
    supportResponsive: true,
    //container: => use @default
    size: 600,
    //viewBoxSize: => use @default
    innerRadius: 600 / 3.5,
    //outerRadius: => use @default
    radiusMin: 50,
    //radiusMax: use @default
    //intersectDelta: use @default
    //intersectInc: use @default
    //circleColor: use @default

    data: {
      items: [{"line": "Blue Line", "avg": 3.43}, {"line": "B Line", "avg": 3.57}, {"line": "C Line", "avg": 3.48}, {"line": "D Line", "avg": 3.41}, {"line": "E Line", "avg": 3.41}, {"line": "Red Line", "avg": 3.24}, {"line": "Orange Line", "avg": 3.11}],
      eval: function (item) {return item.avg;},
      classed: function (item) {return item.line.split(" ").join("");},
    },
    plugins: [
      {
        name: "lines",
        options: {
          format: [
            {// Line #0
              textField: "avg",
              classed: {avg: true},
              style: {
                "font-size": "28px",
                "font-family": "Source Sans Pro, sans-serif",
                "text-anchor": "middle",
                fill: "white"
              },
              attr: {
                dy: "0px",
                x: function (d) {return d.cx;},
                y: function (d) {return d.cy;}
              }
            },
            {// Line #1
              textField: "line",
              classed: {line: true},
              style: {
                "font-size": "14px",
                "font-family": "Source Sans Pro, sans-serif",
                "text-anchor": "middle",
                fill: "white"
              },
              attr: {
                dy: "20px",
                x: function (d) {return d.cx;},
                y: function (d) {return d.cy;}
              }
            }
          ],
          centralFormat: [
            {// Line #0
              style: {"font-size": "50px"},
              attr: {}
            },
            {// Line #1
              style: {"font-size": "30px"},
              attr: {dy: "40px"}
            }
          ]
        }
      }]

  });
});