const urls =  require('./00_config').urls;
const Request = require('request');
const log = require('./93_log_utilities').log;

module.exports.findValDataResource = findValDataResource;
module.exports.getValDataResource = getValDataResource;
module.exports.findValDataProvider = findValDataProvider;
module.exports.getValDataProvider = getValDataProvider;
module.exports.postValDataResource = postValDataResource;
module.exports.putValDataResource = putValDataResource;
module.exports.gbifToValDataset = gbifToValDataset;
module.exports.putValDataProvider = putValDataProvider;
module.exports.postValDataProvider = postValDataProvider;

module.exports.getImages = getImages;
module.exports.processImageArtifacts = processImageArtifacts;

/*
  GET images from VAL image-service API
*/
function getImages(offset=0) {
  var parms = {
    url: `${urls.images}/ws/search?q=*&offset=${offset}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err || res.statusCode > 299) {
        log(1, 'ERROR', 'getValImages', err?err.message:undefined, res?res.statusCode:undefined);
        reject({}); //expecting an object
      } else {
        log(1, 'FOUND', 'getValImages', 'imageCount:', body.images.length, 'totalImageCount:', body.totalImageCount, parms.url, res.statusCode);
        resolve(body);
      }
    });
  });
}

/*
  https://images.vtatlasoflife.org/ws/scheduleArtifactGeneration/c75553df-64a0-4e36-81f3-2a35d615612e -H "apiKey: {apiKey goes here}"
*/
  function processImageArtifacts(idx, imageKey, apiKey) {
    var parms = {
      url: `${urls.images}/ws/scheduleArtifactGeneration/${imageKey}`,
      headers: {"apiKey": apiKey},
      json: true
    };

    return new Promise((resolve, reject) => {
      Request.post(parms, (err, res, body) => {
        if (err || res.statusCode > 299) {
          log(1, 'ERROR', 'processImageArtifacts', idx, imageKey, err?err.message:undefined, res?res.statusCode:undefined);
          reject({}); //expecting an object
        } else {
          log(1, 'SUCCESS', idx, 'processImageArtifacts', parms.url, res.statusCode);
          resolve(body);
        }
      });
    });
  }

/*
  GET dataResource by 'guid' is a search operation returning an array of objects.

  We expect a single match. If more than one is found, throw an error.
*/
function findValDataResource(idx, dKey) {
  var parms = {
    url: `${urls.collectory}/ws/dataResource?guid=${dKey}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err || res.statusCode > 299) {
        log(1,`ERROR | findValDataResource | err:${err?err:undefined} | result:${res?res.statusCode:undefined}`);
        reject({}); //expecting an object
      } else if (1 != body.length) {
        log(1,`ERROR | findValDataResource | FOUND ${body.length} Data Resources`);
        resolve({}); //MUST RESOLVE HERE, DUE TO UNHANDLE PROMISES BY CALLER. Expecting an object.
      } else {
        log(1,`FOUND | findValDataResource | ${idx} | dataset | ${dKey} | ${parms.url} | result: ${JSON.stringify(body[0])}`);
        resolve(body[0]);
      }
    });
  });
}

/*
  GET dataResource from VAL Collectory by drUid returns a single object
*/
function getValDataResource(idx, drUid) {
  var parms = {
    url: `${urls.collectory}/ws/dataResource/${drUid}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err || res.statusCode > 299) {
        log(1,'ERROR', 'getValDataResource', idx, drUid, err?err.message:undefined, res?res.statusCode:undefined);
        reject({}); //expecting an object
      } else {
        log(1,'FOUND', 'getValDataResource', idx, drUid, parms.url, res.statusCode);
        resolve(body);
      }
    });
  });
}

/*
  GET dataProvider by 'guid' is a search operation returning an array of results.

  We expect a single match. If more than one is found, throw an error.

  A successul VAL guid search for dataProvider will return 3 values:

  {
  "name":"Vermont Center for Ecostudies",
  "uri":"https://collectory.vtatlasoflife.org/ws/dataProvider/dp0",
  "uid":"dp0"
  }

  return a single object, empty ({}) or filled
*/
function findValDataProvider(idx, orgKey) {
  var parms = {
    url: `${urls.collectory}/ws/dataProvider?guid=${orgKey}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err || res.statusCode > 299) {
        log(1,`ERROR | findValDataProvider | ${idx} | GBIF Org Key | ${orgKey} | status: ${res?res.statusCode:undefined} | Error: ${err?err.message:undefined}`);
        reject({});
      } else if (1 != body.length) {
        log(1,`ERROR | findValDataProvider | ${idx} | GBIF Org Key | ${orgKey} | FOUND ${body.length} items.`);
        resolve({});
      } else {
        log(1,'FOUND', 'findValDataProvider', idx, 'GBIF Org Key', orgKey, parms.url, body[0]);
        resolve(body[0]);
      }
    });
  });
}

/*
  GET dataProvider from VAL Collectory by dpUid returns a single object
*/
function getValDataProvider(idx, dpUid) {
  var parms = {
    url: `${urls.collectory}/ws/dataProvider/${dpUid}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err || res.statusCode > 299) {
        log(1,`ERROR | getValDataProvider | ${idx} | dpUid | ${dpUid} | error:${err?err.message:undefined} | result:${res?res.statusCode:undefined}`);
        reject({}); //expecting an object
      } else {
        log(1,'SUCCESS', 'getValDataProvider', idx, dpUid, parms.url);
        resolve(body);
      }
    });
  });
}

/*
  POST a new dataResource to VAL Collectory API
*/
function postValDataResource(idx, dKey, gbifDS, dpUid=null, inUid=null, coUid=null) {

  var pBody = gbifToValDataset(gbifDS, dpUid, inUid, coUid); //POST Body - create data format for LA Collectory from GBIF

  var parms = {
    url: `${urls.collectory}/ws/dataResource`,
    body: pBody,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.post(parms, (err, res, body) => {
      if (err || res.statusCode > 299) {
        log(1,'ERROR', 'postValDataResource', idx, dKey, parms.url, dpUid, res?res.statusCode:undefined, err?err.message:undefined);
        reject({});
      } else {
        log(1,'SUCCESS', 'postValDataResource', idx, dKey, parms.url, dpUid, res.statusCode);
        //log(1, idx, body); //What is return from successful POST?
        resolve(body);
      }
    });
  });
}

/*
  PUT existing dataResource to VAL Collectory API

  Inputs:
    - idx loop iterator for display in logs
    - dKey GBIF

  Outputs:
    -
*/
function putValDataResource(idx, dKey, gbifDS, valDR, dpUid=null, inUid=null, coUid=null) {

  var pBody = gbifToValDataset(gbifDS, valDR, dpUid, inUid, coUid); //PUT Body - create data format for LA Collectory from GBIF

  var parms = {
    url: `${urls.collectory}/ws/dataResource/${valDR.uid}`,
    body: pBody,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.put(parms, (err, res, body) => {
      if (err || res.statusCode > 299) {
        log(1, `ERROR | putValDataResource | ${idx} | dataset | ${dKey} |  ${parms.url} | dpUID: ${dpUid} | ${res?res.statusCode:undefined} | ${err?err.message:undefined}`);
        reject({});
      } else {
        log(1, `SUCCESS | putValDataResource | ${idx} | dataset | ${dKey} |  ${parms.url} | dpUID: ${dpUid} | ${res.statusCode}`);
        //log(1, idx, body); //return from successful PUT just a text message.
        resolve(body);
      }
    });
  });
}

/*
  POST a new dataProvider to VAL Collectory API
*/
function postValDataProvider(idx, gbifOrg, gbifIpt={}) {

  var pBody = gbifToValDataProvider(gbifOrg, gbifIpt); //POST Body - create data format for LA Collectory from GBIF

  var parms = {
    url: `${urls.collectory}/ws/dataProvider`,
    body: pBody,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.post(parms, (err, res, body) => {
      if (err) {
        log(1, `ERROR | postValDataProvider | ${idx} | GBIF Org | ${gbifOrg.title} | error: ${err.message} | Req Params:`);
        log(1, parms);
        reject(err);
      } else if (res.statusCode > 299) {
        log(1, `ERROR | postValDataProvider | ${idx} | GBIF Org | ${gbifOrg.title}| Bad Result: ${res.statusCode} | Req Params:`);
        log(1, parms);
        reject(res);
      } else {
        log(1, `SUCCESS | postValDataProvider | ${idx} | GBIF Org | ${gbifOrg.title}| Status: ${res.statusCode}`);
        log(1, body);
        resolve(body);
      }
    });
  });
}

/*
  PUT existing dataProvider to VAL Collectory API
*/
function putValDataProvider(idx, gbifOrg, gbifIpt={}, valDP) {

  var pBody = gbifToValDataProvider(gbifOrg, gbifIpt); //PUT Body - create data format for LA Collectory from GBIF

  var parms = {
    url: `${urls.collectory}/ws/dataProvider/${valDP.uid}`,
    body: pBody,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.put(parms, (err, res, body) => {
      if (err) {
        log(1, `ERROR | putValDataProvider | ${idx} | GBIF Org | ${gbifOrg.title} | VAL dataProvider: ${valDP.uid} | Error: ${err.message} | Req Params:`);
        log(1, parms);
        reject({});
      } else if (res.statusCode > 299) {
        log(1, `ERROR | putValDataProvider | ${idx} | VAL dataProvider: ${valDP.uid} | Bad Result: ${res.statusCode} | Req Params:`);
        log(1, parms);
        reject({});
      } else {
        log(1, `SUCCESS | putValDataProvider | ${idx} | GBIF Org Key | ${gbifOrg.key} | VAL dataProvider: ${valDP.uid} | Status: ${res.statusCode}`);
        //log(1, body);
        resolve(body);
      }
    });
  });
}

/*
  Map GBIF dataSet properties to ALA dataResource properties

  ALA Collectory insertable fields are found here:
  https://github.com/AtlasOfLivingAustralia/collectory-plugin/blob/2ed9737c04db9a07fe9052d40ece43c4e5a2b207/grails-app/services/au/org/ala/collectory/CrudService.groovy#L19

  baseStringProperties =
      ['guid','name','acronym','phone','email','state','pubShortDescription',
      'pubDescription','techDescription','notes', 'isALAPartner','focus','attributions',
      'websiteUrl','networkMembership','altitude', 'street','postBox','postcode','city',
      'state','country','file','caption','attribution','copyright', 'gbifRegistryKey']
      https://github.com/AtlasOfLivingAustralia/collectory-plugin/blob/2ed9737c04db9a07fe9052d40ece43c4e5a2b207/grails-app/services/au/org/ala/collectory/CrudService.groovy#L33

  dataResourceStringProperties =
      ['rights','citation','dataGeneralizations','informationWithheld',
      'permissionsDocument','licenseType','licenseVersion','status','mobilisationNotes','provenance',
      'harvestingNotes','connectionParameters','resourceType','permissionsDocumentType','riskAssessment',
      'filed','publicArchiveAvailable','contentTypes','defaultDarwinCoreValues', 'imageMetadata',
      'geographicDescription','northBoundingCoordinate','southBoundingCoordinate','eastBoundingCoordinate',
      'westBoundingCoordinate','beginDate','endDate','qualityControlDescription','methodStepDescription',
      'gbifDoi']

  NOTE: To debug this, on val core server:
    - tail -f /var/log/tomcat7ala-collecotry.log
*/
function gbifToValDataset(gbifDS, valDR={}, valDP=null, valIN=null, valCO=null) {
  var resourceType = 'records';

  //Some values need processing. ALA can't handle Sampling Event dataSets yet.
  resourceType = gbifDS.type=='CHECKLIST'?'species-list':
                (gbifDS.type=='OCCURRENCE'?'records':
                (gbifDS.type=='SAMPLING_EVENT'?'records':'records'));

  // Don't change all nulls to empty strings (""). Some fields require null or non-empty string.
  var url = `https://www.gbif.org/occurrence/search?dataset_key=${gbifDS.key}&gadm_gid=USA.46_1`;
  var val = {
      "name": `${gbifDS.title} (Vermont)`,
      //"acronym": "",
      "guid": gbifDS.key,
      //"address": gbifDS.contacts[0] ? gbifDS.contacts[0].address[0] : null], //changed from 'street' on 2021-02-16 per debugging of server log /var/log/tomcat7/ala-collectory.log
      "postbox": "",
      "postCode": gbifDS.contacts[0] ? gbifDS.contacts[0].postalCode : null,
      "City": gbifDS.contacts[0] ? gbifDS.contacts[0].city : null,
      "state": gbifDS.contacts[0] ? gbifDS.contacts[0].province : null,
      "Country": gbifDS.contacts[0] ? gbifDS.contacts[0].country : null,
      "phone": gbifDS.contacts[0] ? (gbifDS.contacts[0].phone[0] ? gbifDS.contacts[0].phone[0].substring(0,24) : null ) : null,
      "email": gbifDS.contacts[0] ? gbifDS.contacts[0].email[0] : null,
      "pubShortDescription":  ``,
      "pubDescription": `${gbifDS.description} (Vermont Occurrences | GBIF: ${url})`, //pubDesc handles url as html link.
      "techDescription": '', //url, //`<a href=${url}>${url}</a>`, //techDesc does not handle html.
      "focus": "",
      "websiteUrl": gbifDS.homepage ? gbifDS.homepage : (gbifDS.endpoints[0] ? gbifDS.endpoints[0].url : ""),
      "networkMembership": null, //can't be empty string
      "hubMembership": [],
      "taxonomyCoverageHints": [],
      //"attribution": "", //2021-02-16 17:51:25,568 ERROR [CrudService] Insert failed: No such property: attribution for class: au.org.ala.collectory.DataResource Possible solutions: attributions
      "attributions": [], //gbifDS.contacts,
      "rights": gbifDS.license,
      "licenseType": "",
      "licenseVersion": "",
      "citation": gbifDS.citation.text,
      "resourceType": resourceType,
      "dataGeneralizations": "",
      "informationWithheld": "",
      "permissionsDocument": "",
      "permissionsDocumentType": "Other",
      "contentTypes": [
          "GBIF DataSet import"
      ],
      "connectionParameters": {
          "protocol": "DwCA",
          "url": `${urls.primary}/gbif-split/${gbifDS.key}.zip`,
          "termsForUniqueKey": [
              "gbifID"
          ]
      },
      "hasMappedCollections": false,
      "status": "identified",
      "provenance": "", //can't be null. can be empty string.
      "harvestFrequency": 0,
      //"dataCurrency": null, //not a valid field
      "harvestingNotes": "",
      "publicArchiveAvailable": true,
      //"publicArchiveUrl": `${urls.collectory}/archives/gbif/${valDR.uid}/${valDR.uid}.zip`,
      //"gbifArchiveUrl": `${urls.collectory}/archives/gbif/${valDR.uid}/${valDR.uid}.zip`,
      "downloadLimit": 0,
      "gbifDataset": true,
      "isShareableWithGBIF": true,
      "verified": false,
      "gbifRegistryKey": gbifDS.key,
      "beginDate": gbifDS.temporalCoverages[0] ? gbifDS.temporalCoverages[0].start : null,
      "endDate": gbifDS.temporalCoverages[0] ? gbifDS.temporalCoverages[0].end : null,
      "gbifDoi": gbifDS.doi
      }//end val object

  switch(gbifDS.type) {
    case 'OCCURRENCE':
      val.contentTypes.push("point occurrence data");
      break;
    case 'SAMPLING_EVENT':
      val.contentTypes.push("point occurrence data");
      break;
    case 'CHECKLIST':
      val.contentTypes.push("species-list");
      break;
  }
  val.dataLinks = []; //"dataLinks": ["in0", "co0"],
  if (valDP) {
    val.dataProvider = {"uid": valDP};
  }
  if (valIN) {
    val.institution = {"uid": valIN};
    val.dataLinks.push(valIN);
  }
  if (valCO) {
    val.dataLinks.push(valCO);
  }
  log(1, `NOTICE | gbifToValDataset | dataProvider: ${JSON.stringify(val.dataProvider)} | dataLinks: ${JSON.stringify(val.dataLinks)}`);
  return val;
}

/*
  Map GBIF publishingOrganization properties to ALA dataProvider properties

  ALA Collectory insertable fields are found here:
    https://github.com/AtlasOfLivingAustralia/collectory-plugin/blob/2ed9737c04db9a07fe9052d40ece43c4e5a2b207/grails-app/services/au/org/ala/collectory/CrudService.groovy#L19

  UI: https://www.gbif.org/publisher/b6d09100-919d-4026-b35b-22be3dae7156
  API: http://api.gbif.org/v1/organization/b6d09100-919d-4026-b35b-22be3dae7156

  inputs:
    gbifOrg: object, the result from GBIF Org API (required)
    gbitIpt: object, the result from GBIF Ipt API (optional)
    valDP: object, an existing dataProvider result from the VAL Collectory API (optional)

  outputs:
    valDP: object for POST/PUT body to VAL dataProvider Collectory entity

  NOTE: A POST or PUT will fail if any field is incorrect.
*/
function gbifToValDataProvider(gbifOrg, gbifIpt={}, valDP={}) {
    // Don't change all nulls to empty strings (""). Some fields require null or non-empty string.
    valDP.name = gbifOrg.title;
    valDP.acronym = "";//"VCE",
    valDP.guid = gbifOrg.key;
    //valDP.address = gbifOrg.address;
    //valDP.state = gbifOrg.province?gbifOrg.province:'';
    //valDP.city = gbifOrg.city;
    //valDP.postCode = gbifOrg.postalCode;
    //valDP.country gbifOrg.country;
    valDP.latitude = gbifOrg.latitude;
    valDP.longitude = gbifOrg.longitude;
    valDP.phone = gbifOrg.phone[0];
    valDP.email = gbifOrg.email[0];
    valDP.pubShortDescription = '';
    valDP.pubDescription = gbifOrg.description;
    valDP.techDescription = '';
    valDP.focus = '';
    //default to homePage, then IPT endpoint
    if (gbifOrg.homepage[0]) {
      valDP.websiteUrl = gbifOrg.homepage[0];
    } else if (gbifIpt.key) {
      valDP.websiteUrl = gbifIpt.endpoints[0].url; //"http://ipt.vtecostudies.org/ipt-2.3.5/rss.do"
    }
    //valDP.attribution = "";
    valDP.gbifRegistryKey = gbifOrg.key;

  return valDP;
}
