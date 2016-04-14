function getDataFromReqJC(stuff) {
  var d = [];
  for (var elem in stuff) {
    d.push( [elem, parseInt(stuff[elem][0]), parseInt(stuff[elem][1])] );
  }
  return d;
}

function loadJSONJC(cb, filename) {
  var xhr = new XMLHttpRequest();
  var source = "./physicalData/" + filename
  xhr.open("GET", source, true);
  xhr.onreadystatechange = function() {
    if (xhr.readyState == 4 && xhr.status == 200) {
      var d = JSON.parse(xhr.responseText);
      var c = cb(d);
      console.log(c[1]);
      google.charts.load('current', {
        'packages': ['scatter']
      });
      google.charts.setOnLoadCallback(drawChart);

      function drawChart() {
        var data = new google.visualization.DataTable();
        data.addColumn('string', 'Zip Codes');
        data.addColumn('number', 'Number of Hospitals');
        data.addColumn('number', 'Avg Property Value');
        data.addRows(c);

        var options = {
          chart: {
            title: 'Students\' Final Grades',
            subtitle: 'based on hours studied'
          },
          width: 800,
          height: 500,
          series: {
            0: {axis: 'hours studied'},
            1: {axis: 'final grade'}
          },
          axes: {
            y: {
              'hours studied': {label: 'Hours Studied'},
              'final grade': {label: 'Final Exam Grade'}
            }
          }
        };

        var chart = new google.charts.Scatter(document.getElementById('scatter_dual_y'));

        chart.draw(data, options);

        chart.draw(data, options);
      }
    }
  }
  xhr.send();
};

loadJSONJC(getDataFromReqJC, "jamsValue.json");
