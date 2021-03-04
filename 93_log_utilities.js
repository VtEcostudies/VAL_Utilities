const paths = require('./00_config').paths;
var logDir = paths.logDir
var logName = 'log';
var logStream = null;

module.exports.log = log;
module.exports.init = init;

const fs = require('fs');
const moment = require('moment');

function init(name, dir) {
  if (!logStream) {
    logDir = dir ? dir : logDir;
    logName = name ? name : logName;
    return new Promise((resolve, reject) => {
      logStream = fs.createWriteStream(`${logDir}/${moment().format('YYYYMMDD-HHMMSS')}_${logName}.log`);
      logStream.on('open', () => {resolve(true);})
    });
  }
}

async function log(out, consol=true, name=null, dir=null) {
  try {
    if (!logStream) {
      logDir = dir ? dir : logDir;
      logName = name ? name : logName;
      await init(logName, logDir);
    }
    if (typeof out == 'object') { //handles arrays and objects
      var obj = out;
      out = '';
      for (key in obj) {
        out += `${key}:${obj[key]}\n`;
      }
    }
    if (!logStream || consol) {console.log(out);}
    if (logStream) {logStream.write(out + '\n');}
  } catch(error) {
    console.log(`log error: ${error}`);
  }
}
