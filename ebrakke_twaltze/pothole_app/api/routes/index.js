const express = require('express');
const fs = require('fs');
const crypto = require('crypto');
const exec = require('child_process').exec;
const router = express.Router();

/* GET home page. */
router.get('/', function(req, res, next) {
  var start = req.query.start;
  var end = req.query.end;

  if (start.split(' ').length > 1) {
    start = start.split(' ').join('')
  }
  if (end.split(' ').length > 1) {
    end = end.split(' ').join('');
  }

  var hash = crypto.createHash('sha256').update(start + end).digest('hex');

  var process = exec(`python3 get_incidents_along_route.py ${start} ${end} ${hash}`, (error, stdout, stderr) => {
    if (error) {
      console.log(error);
      res.json({error: 'Something went wrong'});
      return;
    }
    var response = JSON.parse(fs.readFileSync(`data/${hash}-data.json`, 'utf-8'));
    res.json(response);

  });


});

module.exports = router;
