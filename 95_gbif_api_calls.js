const urls =  require('./00_config').urls;
const Request = require('request');
const log = require('./93_log_utilities').log;

module.exports.getGbifDataset = getGbifDataset;
module.exports.getGbifPublisher = getGbifPublisher;
module.exports.getGbifInstallations = getGbifInstallations;
module.exports.getGbifInstallation = getGbifInstallation;

/*
  Use this to get publishingOrganizationKey from datasetKey. Eg.:
  https://api.gbif.org/v1/dataset/f2faaa4c-74e9-457a-8265-06ef5cc73626

  inputs:
    - integer index value of iteration over (to send to logs for easy-reading)
    - string 'dataSetKey' == a GBIF guid used to identify a single dataSet

  outputs:
    - JSON object, empty ({}) or filled.
    - log and console output for success and errors.
*/
function getGbifDataset(idx, dataSetKey) {
  var parms = {
    url: `http://api.gbif.org/v1/dataset/${dataSetKey}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      log(`getGbifDataset | ${idx} | Dataset Key | ${dataSetKey} | ${res.statusCode}`);
      if (err || res.statusCode > 399) {
        reject({});
      } else {
        resolve(body); //in this case the API always returns an object {}, not an array.
      }
    });
  });
}

/*
  GET all GBIF data for an Organization from the GBIF API.

  GBIF publishers are just Organizations with published datasets. Eg.:

  http://api.gbif.org/v1/organization/b6d09100-919d-4026-b35b-22be3dae7156

  Therefore, *if* you obtained an orgKey by getting publishingOrganizationKey
  from a GBIF dataSet, then this orgKey is a 'Publisher'.

  inputs:
  - integer index value of iteration over (to send to logs for easy-reading)
  - string 'orgKey' == a GBIF guid used to identify a single Organization

outputs:
  - JSON object, empty ({}) or filled.
  - log and console output for success and errors.
*/
function getGbifPublisher(idx, orgKey) {
  var parms = {
    url: `http://api.gbif.org/v1/organization/${orgKey}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      log(`GBIF Publisher | ${idx} | Organization Key | ${orgKey} | ${res.statusCode}`);
      if (err || res.statusCode > 399) {
        log(`getGbifPublisher`, err);
        reject({}); //return empty object to allow process to proceed.
      } else {
        resolve(body); //in this case the API always returns an object {}, not an array.
      }
    });
  });
}

/*
  Some GBIF publishing organizations have IPT installations, some do not.

  Get an array of installations (and their data) for an organization like this:

  http://api.gbif.org/v1/organization/b6d09100-919d-4026-b35b-22be3dae7156/installation

  Apparently, an Org can have more than one IPT.

  Get just installation data with an installationKey like this:

  http://api.gbif.org/v1/installation/5ae8b93d-03d9-48f1-8334-f1f251d13f1f

*/
function getGbifInstallations(idx, orgKey, single=false) {
  var parms = {
    url: `http://api.gbif.org/v1/organization/${orgKey}/installation`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      log(`getGbifInstallations | ${idx} | Organization | ${orgKey} | ${res.statusCode}`);
      if (err || res.statusCode > 399) {
        log(`getGbifInstallations`, err);
        if (single) {reject({});}
        else {reject([]);} //return empty array to allow process to proceed.
      } else {
        if (single) {resolve(body[0]);}
        else {resolve(body);} //array of objects
      }
    });
  });
}

function getGbifInstallation(idx, orgKey) {
  return getGbifInstallations(idx, orgKey, true);
}
