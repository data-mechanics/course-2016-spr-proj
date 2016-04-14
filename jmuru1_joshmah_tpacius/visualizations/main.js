"use strict";

function getDataFromReqHJ(stuff) {
  var d = [
    ["Hospital", "Number of jams"]
  ];
  for (var elem in stuff) {
    d.push([elem, stuff[elem]]);
  }
  return d;
}

function loadJSONJH(cb) {
  var xhr = new XMLHttpRequest();
  xhr.open("GET", "./physicalData/jamHospiCount.json", true);
  xhr.onreadystatechange = function() {
    if (xhr.readyState == 4 && xhr.status == 200) {
      var d = JSON.parse(xhr.responseText);
      var c = cb(d);
      console.log(c);
      google.charts.load("current", {
        packages: ["corechart", "bar"]
      });
      google.charts.setOnLoadCallback(drawChart);

      function drawChart() {
        var data = google.visualization.arrayToDataTable(c);
        var options = {
          title: "Traffic Jams at Hospitals in Boston"
        };

        var chart = new google.visualization.BarChart(document.getElementById('bar-chart'));
        chart.draw(data, options);
      }
    };
  }
  xhr.send();
};


loadJSONJH(getDataFromReqHJ);


// var countCorr = -0.0011948720685760594;
// var jamCorr = 0.6722115956157555;
