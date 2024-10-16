//const urls = require('./00_config').urls;
const get = require('request').get; //import { get } from 'request';
const log = require('./93_log_utilities'); //import { log } from './93_log_utilities';

module.exports.getGbifDataset = getGbifDataset;
module.exports.getGbifPublisher = getGbifPublisher;
module.exports.getGbifInstallations = getGbifInstallations;
module.exports.getGbifInstallation = getGbifInstallation;
module.exports.getGbifOccurrence = getGbifOccurrence;
module.exports.getGbifTaxon = getGbifTaxon;

/*
  Use this to get publishingOrganizationKey from datasetKey. Eg.:
  https://api.gbif.org/v1/dataset/f2faaa4c-74e9-457a-8265-06ef5cc73626

  inputs:
    - integer index value of loop counter (to send to logs for easy-reading)
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
      if (err || res.statusCode > 299) {
        log(`ERROR | getGbifDataset | ${idx} | Dataset Key | ${dataSetKey} | ${res?res.statusCode:undefined} | error: ${err?err.message:undefined} | url: ${parms.url}`);
        reject({});
      } else {
        log(`SUCCESS | getGbifDataset | ${idx} | Dataset Key | ${dataSetKey} | ${res.statusCode}`);
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
      if (err || res.statusCode > 299) {
        log(`ERROR | getGbifPublisher | ${idx} | Organization Key | ${orgKey} | ${res?res.statusCode:undefined} | error: ${err?err.message:undefined}`);
        reject({}); //return empty object to allow process to proceed.
      } else {
        log(`SUCCESS | getGbifPublisher | ${idx} | Organization Key | ${orgKey} | ${res.statusCode}`);
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

  inputs:
  - integer index value of iteration over (to send to logs for easy-reading)
  - string 'orgKey' == a GBIF guid used to identify a single Organization
  - boolean 'single' to return array (true) object (false)

  outputs:
    - Array of JSON objects, OR:
    - JSON object, empty ({}) or filled.
    - log and console output for success and errors.
*/
function getGbifInstallations(idx, orgKey, single=false) {
  var parms = {
    url: `http://api.gbif.org/v1/organization/${orgKey}/installation`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err || res.statusCode > 299) {
        log(`ERROR | getGbifInstallations | ${idx} | Organization | ${orgKey} | ${res?res.statusCode:undefined} | error: ${err?err.message:undefined}`);
        if (single) {reject({});}
        else {reject([]);}
      } else {
        log(`SUCCESS | getGbifInstallations | ${idx} | Organization | ${orgKey} | ${res.statusCode}`);
        if (single) {resolve(body[0]);}
        else {resolve(body);} //array of objects
      }
    });
  });
}

/*
  Wrapper for getGbifInstallations to return a single object (the first array element)

  inputs:
    - integer index value of iteration over (to send to logs for easy-reading)
    - string 'orgKey' == a GBIF guid used to identify a single Organization

  outputs:
    - JSON object, empty ({}) or filled.
*/
function getGbifInstallation(idx, orgKey) {
  return getGbifInstallations(idx, orgKey, true);
}

/*
inputs:
  - integer index value of loop counter (to send to logs for easy-reading)
  - string 'occurrenceKey' == a GBIF value (usually numeric) used to identify a single occurrence

outputs:
  - JSON object, empty ({}) or filled.
  - log and console output for success and errors.

*/
function getGbifOccurrence(idx, occurrenceKey) {
  var parms = {
    url: `http://api.gbif.org/v1/occurrence/${occurrenceKey}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err || res.statusCode > 299) {
        log(`ERROR | getGbifOccurrence | ${idx} | Occurrence ID | ${occurrenceKey} | ${res?res.statusCode:undefined} | error: ${err?err.message:undefined} | url: ${parms.url}`);
        reject({});
      } else {
        log(`SUCCESS | getGbifOccurrence | ${idx} | Occurrence ID | ${occurrenceKey} | ${res.statusCode}`);
        resolve(body); //in this case the API always returns an object {}, not an array.
      }
    });
  });
}

/*
  Get a GBIF species with a GBIF species key (key, usageKey, ...taxonKey)
  eg. http://api.gbif.org/v1/species/4334
*/
function getGbifTaxon(idx, key) {

  var parms = {
    url: `http://api.gbif.org/v1/species/${key}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err) {
        log(`getGbifTaxon|err.code: ${err.code}`, logStream);
        err.key = key;
        err.idx = idx;
        reject(err);
      } else {
        log(`${idx} | getGbifTaxon(${key}) | ${res.statusCode} | gbifKey:${body.key?key:undefined}`, logStream, true);
        //body.key = key;
        body.idx = idx;
        resolve(body);
      }
    });
  });
}
