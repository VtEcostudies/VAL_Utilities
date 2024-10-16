const parseCanonicalName = require('./97_utilities').parseCanonicalName;

module.exports.initValObject = initValObject;
module.exports.gbifToValSpecies = gbifToValSpecies;
module.exports.gbifToValDirect = gbifToValDirect;
module.exports.gbifOccSpeciesToValDirect = gbifOccSpeciesToValDirect;
module.exports.gbifToValIngest = gbifToValIngest;
module.exports.getParentKeyFromTreeKeys = getParentKeyFromTreeKeys;
module.exports.parseCanonAuthorFromScientificRank = parseCanonAuthorFromScientificRank;

function initValObject() {

  var val = {
    gbifId:'',taxonId:'',scientificName:'',scientificNameAuthorship:'',
    acceptedNameUsage:'',acceptedNameUsageId:'',taxonRank:'',taxonomicStatus:'',
    parentNameUsage:'',parentNameUsageId:'',
    specificEpithet:'',infraspecificEpithet:'',nomenclaturalCode:'',
    vernacularName:'',taxonRemarks:'',
    kingdom:'',kingdomId:'',phylum:'',phylumId:'',
    class:'',classId:'',order:'',orderId:'',family:'',familyId:'',
    genus:'',genusId:'',species:'',speciesId:'',
    datasetName:'',datasetId:'',bibliographicCitation:'',references:'',
    institutionCode:'',collectionCode:'',establishmentMeans:''
    };

    return val;
}

function gbifToValSpecies(gbif) {
  
  var val = {};
  var nub = gbif.nubKey ? gbif.nubKey : gbif.key; //always use the nubKey if there is one
  
  if (gbif.nubKey && gbif.key != gbif.nubKey) {misCount++;}
  val.key = Number(gbif.key);
  val.nubKey = gbif.nubKey ? Number(gbif.nubKey) : 0;
  val.taxonId = nub;
  val.scientificName = gbif.scientificName;
  val.canonicalName = gbif.canonicalName ? gbif.canonicalName : null;
  val.scientificNameAuthorship = gbif.authorship ? gbif.authorship : null;
  val.acceptedNameUsageId = gbif.acceptedKey;
  val.acceptedNameUsage = gbif.accepted;
  val.taxonRank = gbif.rank;
  val.taxonomicStatus = gbif.taxonomicStatus;
  val.taxonRemarks = gbif.remarks;
  val.parentNameUsageId = gbif.parentKey ? gbif.parentKey : null;
  val.parentNameUsage = gbif.parent ? gbif.parent: null;
  val.vernacularName = gbif.vernacularName ? gbif.vernacularName : null;
  val.kingdom = gbif.kingdom ? gbif.kingdom : null;
  val.kingdomId = gbif.kingdomKey ? gbif.kingdomKey : null;
  val.phylum = gbif.phylum ? gbif.phylum : null;
  val.phylumId = gbif.phylumKey ? gbif.phylumKey : null;
  val.class = gbif.class ? gbif.class : null;
  val.classId = gbif.classKey ? gbif.classKey : null;
  val.order = gbif.order ? gbif.order : null;
  val.orderId = gbif.orderKey ? gbif.orderKey : null;
  val.family = gbif.family ? gbif.family : null;
  val.familyId = gbif.familyKey ? gbif.familyKey : null;
  val.genus = gbif.genus ? gbif.genus : null;
  val.genusId = gbif.genusKey ? gbif.genusKey : null;
  val.species = gbif.species ? gbif.species : null;
  val.speciesId = gbif.speciesKey ? gbif.speciesKey : null;

  /*
    GBIF may not provide 'accepted' or 'acceptedKey' for taxonomicStatus == 'DOUBTFUL', or
    for random taxa. The 'accepted' values do not appear to be reliable at this API endpoint.
    VAL DE requires 'acceptedNameUsage' and 'acceptedNameUsageId', so here we hack those in.
    These anomalies are easy to find in the db. As of 2022-01-27, there were 619 of these:
    select count(*) from val_species where LOWER("taxonomicStatus") like '%doubt%';
    Also: GBIF does not provide accepted when key == nubKey, for obvious reasons, bolstering
    our decision to make these self-referential when they're missing.
 */
  if (!gbif.acceptedKey || !gbif.accepted) {
    val.acceptedNameUsage = gbif.scientificName;
    val.acceptedNameUsageId = nub; //not certain about using nub, here
  }
  if (!gbif.canonicalName) {
    let res = parseCanonicalName(val);
    val.canonicalName = res.canonicalName;
    if (!gbif.authorship) {
      val.scientificNameAuthorship = res.scientificNameAuthorship;
    }
  }
  if ('SPECIES' == gbif.rank) { //pluck dangling token from end of canonicalName by removing @genus...
    const canon = val.canonicalName;
    const genus = gbif.genus;
    val.specificEpithet = (canon.replace(genus, '')).trim();
  }
  if (['SUBSPECIES','VARIETY','FORM'].includes(gbif.rank)) { //species is ALWAYS a 2-token name, so this works by removing @species
    const canon = val.canonicalName;
    const species = gbif.species;
    val.infraspecificEpithet = (canon.replace(species, '')).trim();
  }
  return val;
}

/*
Convert gbif fields to val_species columns for output file and ingestion into
val_species database table.

This is used by output emanating from GBIF species endpoints, not occurrence species_list endpoints.
The fields from those are quite different.
*/
function gbifToValDirect(gbif) {

  var val = initValObject(); //necessary to make field-order consistent across rows and datasets

  if (gbif.canonicalName) {
    var rank = gbif.rank?gbif.rank.toLowerCase():undefined;
    var speciessub = gbif.canonicalName.split(" ").slice(); //break into tokens by spaces
    val.specificEpithet=rank=='species'?speciessub[1]:null;
    val.infraspecificEpithet=rank=='subspecies'?speciessub[2]:null;
    val.infraspecificEpithet=rank=='variety'?speciessub[2]:val.infraspecificEpithet; //don't overwrite previous on false...
    val.infraspecificEpithet=rank=='form'?speciessub[2]:val.infraspecificEpithet; //don't overwrite previous on false...
  }

  val.gbifId=gbif.key;
  val.taxonId=gbif.key;
  val.scientificName=gbif.canonicalName?gbif.canonicalName:gbif.scientificName; //scientificName often contains author. nameindexer cannot handle that, so remove it.
  val.acceptedNameUsageId=gbif.acceptedKey?gbif.acceptedKey:gbif.key;
  val.acceptedNameUsage=gbif.accepted?gbif.accepted:gbif.scientificName;
  val.taxonRank=gbif.rank?gbif.rank.toLowerCase():null;
  val.taxonomicStatus=gbif.taxonomicStatus?gbif.taxonomicStatus.toLowerCase():null;
  val.parentNameUsageId=gbif.parentKey?gbif.parentKey:getParentKeyFromTreeKeys(gbif);
  val.nomenclaturalCode='GBIF';
  val.scientificNameAuthorship=gbif.authorship?gbif.authorship:null;
  val.vernacularName=gbif.vernacularName?gbif.vernacularName:null;
  val.taxonRemarks=gbif.remarks?gbif.remarks:null;

  val.kingdom=gbif.kingdom?gbif.kingdom:null;
  val.kingdomId=gbif.kingdomKey?gbif.kingdomKey:null;;
  val.phylum=gbif.phylum?gbif.phylum:null;
  val.phylumId=gbif.phylumKey?gbif.phylumKey:null;
  val.class=gbif.class?gbif.class:null;
  val.classId=gbif.classKey?gbif.classKey:null;
  val.order=gbif.order?gbif.order:null;
  val.orderId=gbif.orderKey?gbif.orderKey:null;
  val.family=gbif.family?gbif.family:null;
  val.familyId=gbif.familyKey?gbif.familyKey:null;
  val.genus=gbif.genus?gbif.genus:null;
  val.genusId=gbif.genusKey?gbif.genusKey:null;
  val.species=gbif.species?gbif.species:null; //if accepted species: null; if synonym species or a subspecies: accepted species name
  val.speciesId=gbif.speciesKey?gbif.speciesKey:null; //if accepted species: null; if synonym species or a subspecies: accepted species key

  return val;
}

/*
Convert gbif occurrence download SPECIES_LIST file fields to val_species columns for ingestion into
val_species database table.

Created on 2021-05-11. Column headings for that dataset on that date:

index GBIF name
0	taxonKey
1	scientificName
2	acceptedTaxonKey
3	acceptedScientificName
4	numberOfOccurrences
5	taxonRank
6	taxonomicStatus
7	kingdom
8	kingdomKey
9	phylum
10	phylumKey
11	class
12	classKey
13	order
14	orderKey
15	family
16	familyKey
17	genus
18	genusKey
19	species
20	speciesKey
21	iucnRedListCategory

*/
function gbifOccSpeciesToValDirect(gbif) {

  var val = initValObject(); //necessary to make field-order consistent across rows and datasets

  var canonAuthor = parseCanonAuthorFromScientificRank(gbif.scientificName, gbif.taxonRank);
  gbif.canonicalName = canonAuthor.canon;
  gbif.authorship = canonAuthor.author;

  val.gbifId=gbif.taxonKey;
  val.taxonId=gbif.taxonKey;
  val.scientificName=gbif.canonicalName?gbif.canonicalName:gbif.scientificName; //scientificName often contains author. nameindexer cannot handle that, so remove it.
  val.acceptedNameUsageId=gbif.acceptedTaxonKey?gbif.acceptedTaxonKey:gbif.taxonKey;
  val.acceptedNameUsage=gbif.acceptedScientificName?gbif.acceptedScientificName:gbif.scientificName;
  val.taxonRank=gbif.taxonRank?gbif.taxonRank.toLowerCase():null;
  val.taxonomicStatus=gbif.taxonomicStatus?gbif.taxonomicStatus.toLowerCase():null;
  val.parentNameUsageId=gbif.parentKey?gbif.parentKey:getParentKeyFromTreeKeys(gbif);
  val.nomenclaturalCode='GBIF';
  val.scientificNameAuthorship=gbif.authorship?gbif.authorship:null;
  val.vernacularName=gbif.vernacularName?gbif.vernacularName:null;
  val.taxonRemarks=gbif.remarks?gbif.remarks:null;

  val.kingdom=gbif.kingdom?gbif.kingdom:null;
  val.kingdomId=gbif.kingdomKey?gbif.kingdomKey:null;;
  val.phylum=gbif.phylum?gbif.phylum:null;
  val.phylumId=gbif.phylumKey?gbif.phylumKey:null;
  val.class=gbif.class?gbif.class:null;
  val.classId=gbif.classKey?gbif.classKey:null;
  val.order=gbif.order?gbif.order:null;
  val.orderId=gbif.orderKey?gbif.orderKey:null;
  val.family=gbif.family?gbif.family:null;
  val.familyId=gbif.familyKey?gbif.familyKey:null;
  val.genus=gbif.genus?gbif.genus:null;
  val.genusId=gbif.genusKey?gbif.genusKey:null;
  val.species=gbif.species?gbif.species:null; //if accepted species: null; if synonym species or a subspecies: accepted species name
  val.speciesId=gbif.speciesKey?gbif.speciesKey:null; //if accepted species: null; if synonym species or a subspecies: accepted species key

  return val;
}

/*
Translate GBIF taxon data to VAL taxon data for insert/update into database and
output to file.

The format of the incoming (source) data should conform to the output of the GBIF
/species API, not the  GBIF /match API.

inputs:

gbif - object returned from GBIF /species query - best match available
src - object from source input row

outputs:

scientificName without Author - We should never return a scientificName with author included (use authorship for that)
because the ala nameindexer can't handle it.

val_species columns mapped to their GBIF equivalents

additional val_species columns, if present in the incoming dataset.

2021-02-05 Important Note: Need to learn how to handle species and speciesId fields when taxonomicStatus
is '*synonym'.
*/
function gbifToValIngest(gbif, src) {

  var val = initValObject();

  if (src.id) {val.id=src.id;} //spit this back out for incoming DwCA that use it to map val_taxon.txt to other incoming dwca extensions

  //Species API returns key. Fuzzy match API returns usageKey. we handle both, in case this function was called with
  //fuzzy match output. However, the proper approach is to use GBIF match API to search for a match, then use usageKey
  //against GBIF species API to return a complete set of GBIF fields.
  gbif.key=gbif.key?gbif.key:gbif.usageKey;

  val.gbifId=gbif.key;
  val.taxonId=gbif.key;
  val.scientificName=gbif.canonicalName?gbif.canonicalName:src.canonicalName; //scientificName often contains author. nameindexer cannot handle that, so remove it.
  val.acceptedNameUsageId=gbif.acceptedKey?gbif.acceptedKey:gbif.key;
  val.acceptedNameUsage=gbif.accepted?gbif.accepted:gbif.scientificName;
  val.taxonRank=gbif.rank?gbif.rank.toLowerCase():null;
  val.parentNameUsageId=gbif.parentKey || getParentKeyFromTreeKeys(gbif);

  if (gbif.authorship) {
    val.scientificNameAuthorship = gbif.authorship;
  } else if (gbif.canonicalName) {
    var authorsub = gbif.scientificName.split(gbif.canonicalName);
    val.scientificNameAuthorship = authorsub[1]?authorsub[1].trim():null;
    console.log(`Split Author from GBIF scientificName: ${val.scientificNameAuthorship}`);
  }

  if (gbif.canonicalName) {
    var rank = gbif.rank?gbif.rank.toLowerCase():undefined;
    var speciessub = gbif.canonicalName.split(" ").slice(); //break into tokens by spaces
    val.specificEpithet=rank=='species'?speciessub[1]:null;
    val.infraspecificEpithet=rank=='subspecies'?speciessub[2]:null;
    val.infraspecificEpithet=rank=='variety'?speciessub[2]:val.infraspecificEpithet; //don't overwrite previous on false...
  }

  val.nomenclaturalCode='GBIF';
  val.scientificNameAuthorship=val.scientificNameAuthorship?val.scientificNameAuthorship:null;
  val.vernacularName=gbif.vernacularName?gbif.vernacularName:null;
  val.vernacularName=val.vernacularName?val.vernacularName+', ':null+src.vernacularName?src.vernacularName:null;
  //val.vernacularName=src.vernacularName; //temporary override used for a specific import
  src.taxonRemarks=src.taxonRemarks?src.taxonRemarks.trim():null;
  val.taxonRemarks=gbif.remarks?'gbif:'+gbif.remarks:null+src.taxonRemarks?'val:'+src.taxonRemarks:null;
  val.taxonomicStatus=gbif.taxonomicStatus?gbif.taxonomicStatus.toLowerCase():null;

  val.kingdom=gbif.kingdom?gbif.kingdom:null;
  val.kingdomId=gbif.kingdomKey?gbif.kingdomKey:null;;
  val.phylum=gbif.phylum?gbif.phylum:null;
  val.phylumId=gbif.phylumKey?gbif.phylumKey:null;
  val.class=gbif.class?gbif.class:null;
	val.classId=gbif.classKey?gbif.classKey:null;
  val.order=gbif.order?gbif.order:null;
  val.orderId=gbif.orderKey?gbif.orderKey:null;
  val.family=gbif.family?gbif.family:null;
  val.familyId=gbif.familyKey?gbif.familyKey:null;
  val.genus=gbif.genus?gbif.genus:null;
  val.genusId=gbif.genusKey?gbif.genusKey:null;
  if (val.taxonomicStatus.includes('synonym')) {
    val.species=src.species?src.species:null;
    val.speciesId=null;
  } else {
    val.species=gbif.species?gbif.species:null;
    val.speciesId=gbif.speciesKey?gbif.speciesKey:null;
  }

  //append items specific to our species index
  val.datasetName=src.datasetName || null;
  val.datasetId=src.datasetId || null;
  val.bibliographicCitation=src.bibliographicCitation || null;
  val.references=src.references || null;
  val.institutionCode=src.institutionCode || null;
  val.collectionCode=src.collectionCode || null;
  val.establishmentMeans=src.establishmentMeans || null;

  return val;
}


function getParentKeyFromTreeKeys(gbif) {
  var parentId = null;
  var rank = gbif.rank?gbif.rank:(gbif.taxonRank?gbif.taxonRank:null)
  if (!rank) {return null;}

  //parentNameUsageID is key of next higher rank (except for kingdom, which is itself)
  switch(rank.toLowerCase()) {
    case 'kingdom': parentId = gbif.kingdomKey; break;
    case 'phylum': parentId = gbif.kingdomKey; break;
    case 'class': parentId = gbif.phylumKey; break;
    case 'order': parentId = gbif.classKey; break;
    case 'family': parentId = gbif.orderKey; break;
    case 'genus': parentId = gbif.familyKey; break;
    case 'species': parentId = gbif.genusKey; break;
    case 'subspecies': parentId = gbif.speciesKey; break;
    case 'variety': parentId = gbif.speciesKey; break;
  }

  return parentId;
}

/*
  This was an experiment in trying to handle the GBIF species download which accompanies an updated
  GBIF occurrence download by using sciName and taxonomicRank without going back to the GBIF API
  to get 'canonicalName'.

  The GBIF species dwca has odd names in it which should be filtered out, eg. 'BOLD:XXXX'.
  Scientific naming allows multiple ways to refer to subspecies, variety, etc. like:

    - {genus} {species} {subspecies} author
    - {genus} {species} subsp.|var. {subspecies} (author, year)

  That is almost certainly not a complete list, but as of 2021-05-11 it caught all variations
  in the SPECIES_LIST download of an occurrence update.
*/
function parseCanonAuthorFromScientificRank(name, rank) {

  var regex = /  /g;
  if (regex.test(name)) { //critical: replace double spaces with single spaces
    name = name.replace(regex, " ");
  }

  var tokens = name.split(" ").slice(); //break name into tokens by spaces
  var canon = null;
  var author = null;

  console.log('98_gbif_to_val_columns::parseCanonFromScientificRank', name, rank, tokens);

  switch(rank.toLowerCase()) {
    case 'species':
      canon = tokens[0]+' '+tokens[1];
      for (var i=2; i<tokens.length; i++) {
        if (2==i) {author = tokens[i];}
        else {author += ' '+tokens[i];}
      }
      break;
    case 'subspecies':
    case 'variety':
    case 'form':
      switch(tokens[2]) { //sometimes they put 'subsp.' or 'var.' or 'f.' between names
        case 'subsp.':
        case 'var.':
        case 'f.':
          canon = tokens[0]+' '+tokens[1]+' '+tokens[3];
          for (var i=4; i<tokens.length; i++) {
            if (4==i) {author = tokens[i];}
            else {author += ' '+tokens[i];}
          }
          break;
        default:
          canon = tokens[0]+' '+tokens[1]+' '+tokens[2];
          for (var i=3; i<tokens.length; i++) {
            if (3==i) {author = tokens[i];}
            else {author += ' '+tokens[i];}
          }
          break;
      }
      //console.log(`98_gbif_to_val_columns::parseCanonFromScientificRank | ${name} | ${rank} | tokens:`, tokens);
      //console.log(`98_gbif_to_val_columns::parseCanonFromScientificRank | canonicalName:`, canon);
      break;
    default:
      canon = tokens[0];
      for (var i=1; i<tokens.length; i++) {
        if (1==i) {author = tokens[i];}
        else {author += ' '+tokens[i];}
      }
      break;
  }
  //console.log(`parseCanonFromScientificRank | ${name} | ${rank}`, {canon:canon, author:author});
  return {canon:canon, author:author};
}
