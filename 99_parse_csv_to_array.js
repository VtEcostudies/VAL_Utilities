module.exports.csvFileToArrayOfObjects = csvFileTo2DObject; //was this supposed to overload csvFileTo2DArray => csvFileTo2DObject ???
module.exports.csvFileTo2DArray = csvFileTo2DArray;
module.exports.csvTextTo2DArray = csvTextTo2DArray;
module.exports.csvLineTo1DArray = csvLineTo1DArray;
module.exports.csvFileLineByLine = csvFileLineByLine;
module.exports.csvLineToObject = csvLineToObject;

/*
Parsing function copied from one of the answers here:
https://stackoverflow.com/questions/8493195/how-can-i-parse-a-csv-string-with-javascript-which-contains-comma-in-data
Readfile code copied from nodejs help:
https://nodejs.org/api/readline.html#readline_example_read_file_stream_line_by_line
*/
const { once } = require('events');
const { createReadStream } = require('fs');
const { createInterface } = require('readline');

//wrapper for 2DArray function call with headRow
async function csvFileTo2DObject(file, delim=',', headRow=true, filterAscii=false) {
  return csvFileTo2DArray(file, delim, true, filterAscii);
}

/*
  Parse an entire file into an array of objects having key:value pairs if headRow is true.

  Parse an entire file into an array of arrays of values only if headRow is false.
*/
async function csvFileTo2DArray(file, delim=',', headRow=true, filterAscii=false) {
  var idx = 0; //line count. 1 less than total rows if headRow.
  var rows = []; //2D array of rows. rows in array form or object form depending upon header.
  var head = []; //1D array of header field names
  var ret = {"rows":rows, "rowCount":idx, "header":head};

  console.log('csvFileTo2DArray', 'file:',file, 'delim:', delim, 'hasHeader:', headRow, 'filterAscii', filterAscii);

  try {
    const rl = createInterface({
      input: createReadStream(file),
      crlfDelay: Infinity
    });

    rl.on('line', (line) => {
      var rowA = csvLineTo1DArray(line, delim, filterAscii);
      var rowO = {};
      if (headRow && idx > 0 && rowA.length != head.length) {
        throw `ERROR: wrong delimiter count (${rowA.length}) in row ${idx}. Header has ${head.length} delimiters.`;
      }
      if (headRow && idx==0 && head.length==0) {
        head = rowA;
      } else if (headRow) { //convert rowArray to rowObject having key:value pairs
        for (var i=0; i<rowA.length; i++) { //iterate over rowArray
          rowO[head[i]] = rowA[i]; //set rowObject key:value pair
        }
        rows[idx++] = rowO; //set rowsArray object
      } else { //no headrow: build and return rowArray only
        rows[idx++] = rowA;
      }
    });

    await once(rl, 'close');

    console.log(`File ${file} processed and closed.`);

    ret = {
      "rows": rows,
      "rowCount": idx,
      "header": head
    };

    return ret;

  } catch (err) {
    console.log('csvFileTo2DArray', err);
    throw err; //?
  }
};

/*
  Do the same as above, but for very large files that exceed memory limits. Do this
  by receiving a callback function and parsing just one line of the file line-by-line,
  calling that callback for processing line-by-line by the caller.

  Parse an entire file into an array of objects having key:value pairs if headRow is true.

  Parse an entire file into an array of arrays of values only if headRow is false.
*/
async function csvFileLineByLine(file, delim=',', headRow=true, filterAscii=false, callBack={}) {
  var idx = 0; //line count. 1 less than total rows if headRow.
  var head = [];
  var ret = {"rowCount":idx, "header":head};

  console.log('csvFileLineByLine', file, delim, headRow, filterAscii);

  try {
    const rl = createInterface({
      input: createReadStream(file),
      crlfDelay: Infinity
    });

    rl.on('line', (line) => {
      //console.log(`${idx}: ${line}`);
      var rowA = csvLineTo1DArray(line, delim, filterAscii);
      var rowO = {};
      if (headRow && idx > 0 && rowA.length != head.length) {
        throw `ERROR: wrong delimiter count (${rowA.length}) in row ${idx}. Header has ${head.length} delimiters.`;
      }
      if (headRow && idx==0 && head.length==0) {
        head = rowA;
        callBack(head, idx++); //now with line-by-line, pass the header back first
      } else if (headRow) { //convert rowArray to rowObject having key:value pairs
        for (var i=0; i<rowA.length; i++) { //iterate over rowArray
          rowO[head[i]] = rowA[i]; //set rowObject key:value pair
        }
        callBack(rowO, idx++); //send the rowsArray object
      } else { //no headrow: build and send rowArray only
        callBack(rowA, idx++);
      }
    });

    await once(rl, 'close');

    console.log(`File ${file} processed and closed.`);

    ret = {
      "rowCount": idx,
      "header": head
    };

    return ret;

  } catch (err) {
    console.log('csvFileLineByLine', err);
    throw err; //?
  }
};

/*
  Convert one delimited row to JSON object using a fieldName map, 'head', as in
  a .txt or .csv file's header row.

  Return line of data in 3 formats:
    ret.rowA - ordered array of values
    ret.rowO - JSON object of key:value pairs using the header row for keys
    ret.rowT - a reconstructed line of delimited text with values fixed-up (eg. empty literal "" replaced with null)
*/
function csvLineToObject(delim=',', head='', line='', filterAscii=false) {
  var arrH = csvLineTo1DArray(head, delim, filterAscii);
  var rowA = csvLineTo1DArray(line, delim, filterAscii);
  var rowO = {}; //JSON object of line
  var rowT = ''; //text version of line, fixed-up
  var ret = {};

  if (rowA.length != arrH.length) {
    throw `ERROR: wrong delimiter count (${rowA.length}) in row ${idx}. Header has ${arrH.length} delimiters.`;
  }

  for (var i=0; i<rowA.length; i++) { //iterate over rowArray
    rowO[arrH[i]] = rowA[i]; //set rowObject key:value pair
    rowT += rowA[i]+delim; //build new text line from fixed-up values
  }

  ret.rowO = rowO;
  ret.rowA = rowA;
  ret.rowT = rowT.slice(0,-1); //remove trailing delimiter from text row
  return ret;
}

function csvLineTo1DArray(text, delim=',', filterAscii=false) {
    let p = '', row = [''], ret = [row], i = 0, s = !0, l;
    for (l of text) {
        //if ('"' === l) { //this used to be 1st condition
        if (delim === l && s) { //putting delim check before dbl-quote check fixes unbalanced-quote errors
          if ('""' == row[i]) {row[i] = '';} //remove empty double-quotes
          row[i] = row[i].replace(/\s+/g, " "); //replace whitespace with actual space
          if (filterAscii) row[i] = row[i].replace(/[^ -~]+/g, ""); //filter ASCII
          row[i] = row[i].trim();
          l = row[++i] = '';
        }
        else if ('\n' === l && s) {
            row[i] = row[i].replace(/\s+/g, " "); //replace whitespace with actual space
            if (filterAscii) row[i] = row[i].replace(/[^ -~]+/g, ""); //filter ASCII
            row[i] = row[i].trim();
            break; //exit loop at line-ending char
        }
        // else if (delim === l && s) { //this used to be 2nd elseif
        else if ('"' === l) { //this allows verbatim delimiters within double-quoted strings
          if (s && l === p) {row[i] += l;}
          s = !s; //when this condtion is checked 1st, this flag causes unbalanced quotes to override delimiters, an error
        }
        else {
          row[i] += l;
        }
        p = l;
    }
    return row;
};

function csvTextTo2DArray(text, delim=',', filterAscii=false) {
    let p = '', row = [''], ret = [row], i = 0, r = 0, s = !0, l;
    for (l of text) {
        if (delim === l && s) {
          row[i] = row[i].replace(/\s+/g, " "); //replace whitespce with actual space
          if (filterAscii) row[i] = row[i].replace(/[^ -~]+/g, ""); //filter ASCII
          l = row[++i] = '';
        }
        else if ('\n' === l && s) {
            if ('\r' === p) row[i] = row[i].slice(0, -1);
            row[i] = row[i].replace(/\s+/g, " "); //replace whitespce with actual space
            if (filterAscii) row[i] = row[i].replace(/[^ -~]+/g, ""); //filter ASCII
            row[i] = row[i].trim();
            row = ret[++r] = [l = '']; i = 0;
        }
        else if ('"' === l) { //this allows verbatim delimiters within double-quoted strings
            if (s && l === p) row[i] += l;
            s = !s;
        }
        else row[i] += l;
        p = l;
    }
    return ret;
};

function original_csvTextTo2DArray(text, delim=',', filterAscii=false) {
    let p = '', row = [''], ret = [row], i = 0, r = 0, s = !0, l;
    for (l of text) {
        if ('"' === l) {
            if (s && l === p) row[i] += l;
            //s = !s; //this flag causes unbalanced quotes to override delimiters, which is plain wrong
        } else if (delim === l && s) {
          row[i] = row[i].replace(/\s+/g, " "); //replace whitespce with actual space
          if (filterAscii) row[i] = row[i].replace(/[^ -~]+/g, ""); //filter ASCII
          l = row[++i] = '';
        } else if ('\n' === l && s) {
            if ('\r' === p) row[i] = row[i].slice(0, -1);
            row[i] = row[i].replace(/\s+/g, " "); //replace whitespce with actual space
            if (filterAscii) row[i] = row[i].replace(/[^ -~]+/g, ""); //filter ASCII
            row[i] = row[i].trim();
            row = ret[++r] = [l = '']; i = 0;
        } else row[i] += l;
        p = l;
    }
    return ret;
};
