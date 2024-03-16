const delay = require('util').promisify(setTimeout);
const fs = require('fs');

process.on( 'SIGINT', function() {
  console.log( "\nGracefully shutting down from SIGINT (Ctrl-C)" );
  // some other closing procedures go here
  process.exit( );
})

process.on( 'SIGHUP', function() {
  console.log( "\nGracefully shutting down from SIGHUP (Hup!)" );
  // some other closing procedures go here
  process.exit( );
})


process.on( 'SIGTERM', function() {
  console.log( "\nGracefully shutting down from SIGTERM (bleh!)" );
  // some other closing procedures go here
  process.exit( );
})

async function main(){
    let LOG_FILE = process.env.LOG_FILE || 'sample.log';
    let DELAY_MS = process.env.DELAY_MS || '1000';
    let delayTime = parseInt(DELAY_MS);

    // read the log file wholly into memory
    let log = fs.readFileSync(LOG_FILE, 'utf8');
    log = log.split('\n');
    let logLength = log.length;
    // the logIndex starts randomly between 0 and logLength
    let logIndex = Math.floor(Math.random() * logLength);

    while (true){
        console.log(log[logIndex]);
        logIndex = (logIndex + 1) % logLength;
        await delay(DELAY_MS);
    }
}

main();