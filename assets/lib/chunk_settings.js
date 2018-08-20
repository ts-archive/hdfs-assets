'use strict';


/* This module is a set of functions that can be used for general files readers. These functions
 * will determine the various settings needed for reading files chunks
 */

// This function sets up the options object that will be passed down the read promise chain
function getReadOptions(slice, opConfig) {
    const chunkOptions = {};
    chunkOptions.needMargin = false;
    chunkOptions.delimiter = opConfig.line_delimiter;
    chunkOptions.path = slice.path;
    // Set up a spot to store the data from the read
    chunkOptions.data = {};
    chunkOptions.options = {};
    if (slice.length) {
        chunkOptions.options.offset = slice.offset;
        chunkOptions.options.length = slice.length;
        // Determines whether or not to grab the extra margin.
        if (slice.offset + slice.length !== slice.total) {
            chunkOptions.needMargin = true;
        }
    }
    return chunkOptions;
}

function averageRecordSize(array) {
    return Math.floor(array.reduce((accum, str) => accum + str.length, 0) / array.length);
}

// This function checks to see if the margin is needed for the chunk and adds the extra margin read
// options if so
function checkMargin(chunkOptions) {
    const finalChar = chunkOptions.data[chunkOptions.data.length - 1];
    // Skip the margin if the raw data ends with a newline since it will end with a complete
    // record
    if (finalChar === chunkOptions.delimiter) {
        chunkOptions.needMargin = false;
    }
    if (chunkOptions.needMargin) {
        const avgSize = averageRecordSize(chunkOptions.data.split('\n'));
        chunkOptions.marginOptions = {};
        // Safety margin of two average-sized records
        chunkOptions.marginOptions.length = avgSize * 2;
        chunkOptions.marginOptions.offset = chunkOptions.options.offset
            + chunkOptions.options.length;
    }
    return chunkOptions;
}


// This function takes the raw data and breaks it into records, getting rid of anything preceding
// the first complete record if the data does not start with a complete record. It just returns an
// array of the records contained in the data
function cleanData(chunkOptions) {
    /* Since slices with a non-zero chunk offset grab the character immediately preceding the main
     * chunk, if one of those chunks has a delimiter as the first or second character, it means the
     * chunk starts with a complete record. In this case as well as when the chunk begins with a
     * partial record, splitting the chunk into an array by its delimiter will result in a single
     * garbage record at the beginning of the array. If the offset is 0, the array will never start
     * with a garbage record
     */

    let outputData = chunkOptions.data;
    // Get rid of last character if it is the delimiter since that will just result in an empty
    // record
    if (outputData[outputData.length - 1] === chunkOptions.delimiter) {
        outputData = outputData.substring(0, outputData.length - 1);
    }

    if (chunkOptions.options.offset === 0) {
        // Return everthing
        return outputData.split(chunkOptions.delimiter);
    }

    return outputData.split(chunkOptions.delimiter).splice(1);
}

module.exports = {
    getReadOptions,
    checkMargin,
    cleanData
};
