const paths = require('./00_config').paths;
var logDir = paths.logDir
var logName = 'log';
var logStream = null;

module.exports.log = log;
module.exports.init = init;

const fs = require('fs');
const moment = require('moment');

/*
  Initialize log output to file. Call this before the first call to log() below.

  Inputs:
    - name: log file name, *without* extension
    - dir: log file directory

  Outputs:
    - set file-scope var logStream for use by subsequent calls to log() below

  Notes:
    - fs.createWriteStream is asynchronous
    - use init() with acync await to wait for logStream to be ready for use
    - if you do not await, you will get multple calls to init(), and multiple files
*/
function init(name=null, dir=null) {
  return new Promise((resolve, reject) => {
    if (!logStream) {
      logDir = dir ? dir : logDir;
      logName = name ? name : logName;
        logStream = fs.createWriteStream(`${logDir}/${moment().format('YYYYMMDD-HHMMSS')}_${logName}.log`);
        logStream.on('open', () => {resolve(true);});
        logStream.on('error', (err) => {
          console.log('93_log_utilities | init fs.createWriteStream | ERROR:', err);
          logStream = null;
          resolve(err); //if we reject here, try..catch in log() traps this error so we can't proceed.
        });
      } else {
        resolve(true);
      }
    });
}

/*
  log output to file and/or console

  log() now handles a variable argument list. It searches that list for these objects
    - {console: true/false} turns on/off console output for this specific call to log()
*/
async function log(...args) {
  var consol = true; //flag console output. In large, fast processing on Windows, console is limiting speed factor. Not true on Linux.
  var output =  ''; //output for one arg's value(s)
  var result = null; //accept the result of the init() operation
  try {
    if (!logStream) {result = await init();} //init always returns success. it not, try->catch traps this as error.
    output = moment().format('YYYY-MM-DD_HH:MM:SS');
    for (var i = 0; i < args.length; i++) {
      if (typeof args[i] == 'object') {
        if (args[i]['console']) {consol = args[i].console;}
        else {output += ' | ' + JSON.stringify(args[i]);}
      } else {
        output += ' | ' + args[i];
      }
    }
    if (!logStream || consol) {console.log(output);}
    if (logStream) {logStream.write(output + '\n');}
  } catch(error) {
    console.log(`ERROR | caught in log() |`, error);
  }
}

/*
  Test variable args and how they work.
*/
function test(out, consol=true, ...args) {
  console.log('out:', out);
  console.log('consol:', consol);
  for (var i = 0; i < arguments.length; i++) {
    console.log('arguments', i, arguments[i]);
  }
  var txt = '';
  for (var i = 0; i < args.length; i++) {
    txt = JSON.stringify(args[i]);
    console.log('args', i, args[i], txt);
  }
  log(...args);
}

//test test and log output
//test({"one":1, "two":2}, 'three', {"console": true}, 4, ['five', 'six']);
//log({"one":1, "two":2}, 'three', {"console": true}, 4, ['five', 'six']);
