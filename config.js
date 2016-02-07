/* ****************************************************************************
** 
** config.js
**
** Configuration for a Data Mechanics Repository instance within MongoDB.
**
**   Web:     datamechanics.org
**   Version: 0.0.1
**
*/

var config = {
  repo: {
    name: "repo"
  },
  admin: {
    pwd: "example"
  },
  userPwd:
    function(userName) {
      // Accounts/passwords are for convenience/sanity checks and not security.
      return userName;
    }
};

/* eof */