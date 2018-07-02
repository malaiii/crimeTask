
const fileStream = require('fs');
const readStream = require('readline');


let getLines = function getLines (filename, callback) {
    const readFStream = fileStream.createReadStream(filename, 'UTF-8');
    const readInterface = readStream.createInterface({
        input: readFStream,
    });

    const crimesDataStreamTheft = fileStream.createWriteStream('crimesdata-theft.json', {
        flags: 'a',
    });
    
    const crimesDataStreamAssult = fileStream.createWriteStream('crimesdata-assault.json', {
    flags: 'a',
    });
    
    readInterface.on('line', (input) => {
        i += 1;
        if (i === 1) {
        headers = input.split(',');
        } else {
        readInterface.pause(); 
        const dobj = {};
        const dataEl = input.split(',');
        for (let j = 0; j < headers.length; j += 1) {
            dobj[headers[j]] = dataEl[j];
        }
    
        if (dobj['Primary Type'] === 'THEFT') {
            processTheft(dobj);
        }
    
        if (dobj['Primary Type'] === 'ASSAULT') {
            processAssault(dobj);
        }
        readInterface.resume();
        }
    });

    // One the completion of file read, write the JSON files
    readInterface.on('close', () => {
        crimesDataStreamTheft.on('finish', () => {
            crimesDataStreamTheft.close();
        });
        crimesDataStreamTheft.write(JSON.stringify(theftAggrData));
    
        crimesDataStreamAssult.on('finish', () => {
            crimesDataStreamAssult.close();
        });
        crimesDataStreamAssult.write(JSON.stringify(assaultAggrData));
        callback("Json Created");
    });

    readInterface.on("error", function () {
        callback("Error");
    });
}

let i = 0;
let j = 0;

let headers = []; // Array to store the headers from csv
const theftAggrData = {}; //  Theft data object to be written to JSON file
const assaultAggrData = {}; //  Assault data object to be written to JSON file

// Object to define year range
const yearRange = {
  start: 2001,
  end: 2018,
};

function processAssault(data) {
    j += 1;
    if (j % 100000 === 0) {
        process.stdout.write(',');
    }
    const yr = data.Year;
    if (yr >= yearRange.start && yr <= yearRange.end) {
        if (!assaultAggrData[yr]) {
        assaultAggrData[yr] = {
            arrestCount: 0,
            nonArrestCount: 0,
            year: yr,
        };
        }

        if (data.Arrest === 'false') {
        assaultAggrData[yr].nonArrestCount += 1;
        } else {
        assaultAggrData[yr].arrestCount += 1;
        }
    }
}
  
function processTheft(data) {
    i += 1;
    if (i % 100000 === 0) {
        process.stdout.write('.');
    }
    const yr = data.Year;
    if (yr >= yearRange.start && yr <= yearRange.end) {
        if (!theftAggrData[yr]) {
        theftAggrData[yr] = {
            over500: 0,
            under500: 0,
            year: yr,
        };
        }

        if (data.Description == '$500 AND UNDER') {
            theftAggrData[yr].under500 += 1;
        } else if (data.Description == 'OVER $500') {
            theftAggrData[yr].over500 += 1;
        }
    }
}

getLines("Crimes_-_2001_to_present.csv", function (err, message) {
    console.log(err);
    console.log(message);
});

