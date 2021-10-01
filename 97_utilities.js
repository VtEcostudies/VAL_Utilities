module.exports.log = log;
module.exports.logErr = logErr;
module.exports.jsonToString = jsonToString;
module.exports.addTaxonRank = addTaxonRank;
module.exports.addCanonicalName = addCanonicalName;
module.exports.parseCanonicalName = parseCanonicalName;

function log(out, stream=null, consoul=false) {
  if (consoul || !stream) {console.log(out);}
  if (stream) stream.write(`${out}\n`);
}

function logErr(out, stream=null, override=true) {
  log(out, stream, override);
}

/*
Convert json object to object with:
  columns: string of column names, separated by delim
  values: string of values, separated by delim
Returned as object, like {columns:'one,two,three', values:'1,2,3'}
*/
function jsonToString(obj, delim=',', stream=null) {
  var vals = ''; //string of column values, delim-separated
  var cols = ''; //string of column names, delim-separated

  try {
    //loop through values. add double quotes if not there
    for (const key in obj) {
      fld = obj[key]!=null?obj[key]:'';
      if (isNaN(fld)) { //check for null, numeric
        //check for leading and trailing double-quotes
        if (fld.substring(0,1) != `"` && fld.substring(fld.length-1,fld.length) != `"`) {
          fld = `"${fld}"`;
        }
      }
      vals += fld + delim;
      cols += key + delim;
    }
    vals = vals.replace(/(^,)|(,$)/g, "");//remove leading, trailing delimiter
    cols = cols.replace(/(^,)|(,$)/g, "");//remove leading, trailing delimiter

    return {values:vals, columns:cols};
  } catch(err) {
    log(`ERROR in jsonToString: ${err.message}`, stream, true);
    return {values:null, columns:null};
  }
}

function addTaxonRank(src, stream=null) {
  try {
    if (!src.canonicalName) {addCanonicalName(src, stream);}

    log(`addTaxonRank for canonicalName ${src.canonicalName}`, stream);

    var tokens = src.canonicalName.split(" ").slice(); //break name into tokens by spaces

    switch(tokens.length) {
      case 3:
        if (src.kingdom && src.kingdom.toLowerCase()=='plantae') {src.taxonRank = 'variety';}
        else {src.taxonRank = 'subspecies';}
        break;
      case 2:
        src.taxonRank = 'species';
        break;
      case 1:
        src.taxonRank = 'genus';
        break;
      default:
        src.taxonRank = 'error';
        throw `Wrong number of tokens (${tokens.length}) in scientificName '${src.canonicalName}'. taxonRank: ${src.taxonRank}`;
        break;
    }
    return src.taxonRank;
  } catch(err) {
    log(`ERROR in addTaxonRank: ${err.message}`, stream);
    src.taxonRank = 'error';
    return src.taxonRank;
  }
}

function addCanonicalName(src, stream=null, silent=1) {
  try {
    log(`addCanonicalName for scientificName ${src.scientificName}`, stream);
    src.canonicalName = parseCanonicalName(src, stream).canonicalName;
    return src.canonicalName;
  } catch(err) {
    log(`ERROR IN addCanonicalName for scientificName ${src.scientificName}: ${err.message}`, stream, true);
    throw `addCanonicalName: ${err}`;
    return null;
  }
}

/*
  Do our own, native parsing of GBIF's concept, canonicalName, from scientificName.

  Input: a source object containing, at least, src.scientificName.
  Output: an object with two keys whose values are our best effort to parse canonicalName from scientificName
    {
    canonicalName: {value}
    scientificNameAuthoriship: {value}
    }
*/
function parseCanonicalName(src, stream=null, silent=1) {
  try {
    if (!silent) log(`parseCanonicalName for scientificName ${src.scientificName}`, stream);
    if (!silent) log(`------------------------------------------------------------`, stream);
    var sciName = src.scientificName;
    var sciAuth = src.scientificNameAuthorship;
    var canName = sciName;
    var regex = /\s+/g;

    canName.replace(/\s+/g, " "); //replace whitespace with actual space

    //find and remove subspecies/variety indentifiers from scientificName
    regex = /( var. )|( variety )|( subsp. )|( spp. )|( ssp. )|( subspecies )/g;
    if (regex.test(canName)) {
      canName = canName.replace(regex, " ");
      if (!silent) log(`parseCanonicalName | Remove ( var. )|( spp. )|( ssp. )|( variety )|( subspecies ) | Input:${sciName} | Output:${canName}`, stream);
      sciName = canName;
    }

    //find and remove ' x ' or ' X ' from name
    regex = /( x )|( X )/g;
    if (regex.test(canName)) {
      canName = canName.replace(regex, " ");
      if (!silent) log(`parseCanonicalName| Remove ( x )|( X ) | Input:${sciName} | Output:${canName}`, stream);
      sciName = canName;
    }

    //find and remove ' sp. ' and ' nr.' from name
    regex = /( sp. )|( nr. )/g;
    if (regex.test(canName)) {
      canName = canName.replace(regex, " ");
      if (!silent) log(`parseCanonicalName| Remove ( sp. )|( nr. ) | Input:${sciName} | Output:${canName}`, stream);
      sciName = canName;
    }

    //find and remove parenthetical Author from scientificName
    regex = /\(.*\)/g;
    if (regex.test(canName)) {
      sciAuth = canName.match(regex)[0];
      canName = canName.replace(regex, " ");
      if (!silent) log(`parseCanonicalName | Remove Parenthetical Names \(.*\) | Input:${sciName} | Output:${canName} | Author:${sciAuth}`, stream);
      sciName = canName;
    }

    //remove ' Author, XXXX ' without parentheses
    regex = /[a-zA-Z]+, [0-9]{4}/
    if (regex.test(canName)) {
      sciAuth = canName.match(regex)[0];
      canName = canName.replace(regex, " ");
      if (!silent) log(`parseCanonicalName| Remove  Author, XXXX / [a-zA-Z]+, [0-9]{4} / | Input:${sciName} | Output:${canName} | Author: ${sciAuth}`, stream);
      sciName = canName;
    }

    //remove numbers
    //regex = /[\d-]/g; //this removes dashes (-), which is bad
    regex = /(0)|(1)|(2)|(3)|(4)|(5)|(6)|(7)|(8)|(9)/g;
    if (regex.test(canName)) {
      canName = canName.replace(regex, "");
      if (!silent) log(`parseCanonicalName| Remove Numbers | Input:${sciName} | Output:${canName}`, stream);
      sciName = canName;
    }

    //remove double spaces
    regex = /  /g;
    if (regex.test(canName)) {
      canName = canName.replace(regex, " ");
      if (!silent) log(`parseCanonicalName| Remove double-spaces | Input:${sciName} | Output:${canName}`, stream);
      sciName = canName;
    }

    //remove leading, trailing spaces
    canName = canName.trim();

    /*
      After all that, there are still cases not handled. eg.
      The simplest way to fix this is to use taxonRank and parse leading tokens,
      which are assumed to be canonicalName tokens.

      NOTE that for this last step to work, we must remove non-name tokens like
      eg. 'subsp.', 'var.', etc.
    */
    var rank = undefined;
    if (typeof src.taxonRank == "string") {rank = src.taxonRank.toLowerCase();}
    if (typeof src.rank == "string") {rank = src.rank.toLowerCase();}
    if (typeof rank != undefined) {
      var toks = canName.split(' ');
      switch (rank) {
        case 'species':
          canName = toks[0] + ' ' + toks[1];
          if (!sciAuth) {sciAuth = toks[2]; for (i=3; i<toks.length; i++) {sciAuth += ` ${toks[i]}`;}}
          break;
        case 'subspecies':
        case 'variety':
          canName = toks[0] + ' ' + toks[1] + ' ' + toks[2];
          if (!sciAuth) {toks[3]; for (i=4; i<toks.length; i++) {sciAuth += ` ${toks[i]}`;}}
          break;
        case 'kingdom':
        case 'phylum':
        case 'class':
        case 'order':
        case 'family':
        case 'genus':
          canName = toks[0];
          if (!sciAuth) {toks[1]; for (i=2; i<toks.length; i++) {sciAuth += ` ${toks[i]}`;}}
          break;
      }
      if (!silent) log(`parseCanonicalName| FINAL PARSE BY RANK: ${rank} | Input:${sciName} | Output:${canName} | Author: ${sciAuth}`, stream);
    }

    return {canonicalName:canName, scientificNameAuthorship:sciAuth};

    } catch(err) {
      log(`ERROR IN parseCanonicalName for scientificName ${src.scientificName}: ${err.message}`, stream, true);
      throw `parseCanonicalName: ${err}`;
      return canName;
  }
}
