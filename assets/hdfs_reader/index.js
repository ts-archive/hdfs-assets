'use strict';

const Promise = require('bluebird');
// const path = require('path');
const Queue = require('@terascope/queue');
// const fs = require('fs');


function getClient(context, config, type) {
    const clientConfig = {};
    clientConfig.type = type;

    if (config && config.connection) {
        clientConfig.endpoint = config.connection ? config.connection : 'default';
        clientConfig.cached = config.connection_cache !== undefined
            ? config.connection_cache : true;
    } else {
        clientConfig.endpoint = 'default';
        clientConfig.cached = true;
    }

    return context.foundation.getConnection(clientConfig);
}

const parallelSlicers = false;

function newSlicer(context, executionContext, retryData, logger) {
    const opConfig = executionContext.config.operations[0];
    const clientService = getClient(context, opConfig, 'hdfs_ha');
    const hdfsClient = clientService.client;
    const queue = new Queue();

    function processFile(file, filePath) {
        const totalLength = file.length;
        let fileSize = file.length;

        if (fileSize <= opConfig.size) {
            queue.enqueue({ path: `${filePath}/${file.pathSuffix}`, fullChunk: true });
        } else {
            let offset = 0;
            while (fileSize > 0) {
                const length = fileSize > opConfig.size ? opConfig.size : fileSize;
                // This grabs the character immediately before the chunk to help check whether or
                // not the chunk starts with a complete record
                let startSpot = offset;
                if (offset > 0) {
                    startSpot = offset - 1;
                }
                queue.enqueue({
                    path: `${filePath}/${file.pathSuffix}`,
                    offset: startSpot,
                    length,
                    total: totalLength
                });
                fileSize -= opConfig.size;
                offset += length;
            }
        }
    }

    function getFilePaths(filePath) {
        return hdfsClient.listStatusAsync(filePath)
            .then(results => Promise.map(results, (metadata) => {
                if (metadata.type === 'FILE') {
                    return processFile(metadata, filePath);
                }
                if (metadata.type === 'DIRECTORY') {
                    return getFilePaths(`${filePath}/${metadata.pathSuffix}`);
                }
                return true;
            }))
            .return([() => queue.dequeue()])
            .catch(err => Promise.reject(parseError(err)));
    }

    return getFilePaths(opConfig.path)
        .catch((err) => {
            const errMsg = parseError(err);
            logger.error(`Error while reading from hdfs, error: ${errMsg}`);
            return Promise.reject(errMsg);
        });
}


function newReader(context, opConfig) {
    const formatters = {
        jsonLines: (data, logger) => data.map(record => JSON.parse(record))
            .filter(element => element !== undefined).catch((err) => {
                logger.error(`There was an error processing the record: ${err}`);
            }),
    };
    // Set up a logger
    const logger = context.apis.foundation.makeLogger({ module: 'hdfs_reader' });
    const clientService = getClient(context, opConfig, 'hdfs_ha');
    const hdfsClient = clientService.client;
    const chunkFormater = formatters[opConfig.format];

    return function processSlice(slice) {
        // Hardcoding the line delimiter, but this could be arbitrary
        // TODO paramaterize this in the config
        const lineDelimiter = '\n';
        return getChunk(hdfsClient, slice, lineDelimiter)
            .then(data => cleanData(data, lineDelimiter, slice))
            .then(results => chunkFormater(results, logger))
            .catch(err => Promise.reject(parseError(err)));
    };

    // return function readChunk(slice) {
    //     return determineChunk(hdfsClient, slice, logger)
    //         .then(results => chunkFormater(results, logger))
    //         .catch((err) => {
    //             const errMsg = parseError(err);
    //             logger.error(errMsg);
    //             return Promise.reject(err);
    //         });
    // };
}

function parseError(err) {
    if (err.message && err.exception) {
        return `Error while reading from HDFS, error: ${err.exception}, ${err.message}`;
    }
    return `Error while reading from HDFS, error: ${err}`;
}

// This function will grab the chunk of data specified by the slice plus an extra margin if the
// slice is not at the end of the file. It needs to read the file twice, first grabbing the data
// specified in the slice and then the margin, which gets appended to the data
function getChunk(hdfsClient, slice, delimiter) {
    function averageRecordSize(array) {
        return Math.floor(array.reduce((accum, str) => accum + str.length, 0) / array.length);
    }

    function getMargin(marginOpts) {
        // This grabs the start of the margin up to the first delimiter, which should be the
        // remainder of a truncated record
        return hdfsClient.openAsync(slice.path, marginOpts)
            .then(data => data.split(delimiter)[0]);
    }


    let needMargin = false;
    const options = {};
    if (slice.length) {
        options.offset = slice.offset;
        options.length = slice.length;
        // Determines whether or not to grab the extra margin.
        if (slice.offset + slice.length !== slice.total) {
            needMargin = true;
        }
    }

    return hdfsClient.openAsync(slice.path, options)
        .then((data) => {
            const finalChar = data[data.length - 1];
            // Skip the margin if the raw data ends with a newline since it will end with a complete
            // record
            if (finalChar === '\n') {
                needMargin = false;
            }
            if (needMargin) {
                const avgSize = averageRecordSize(data.split('\n'));
                const marginOptions = {};
                // Safety margin of two average-sized records
                marginOptions.length = avgSize * 2;
                marginOptions.offset = options.offset + options.length;
                return getMargin(marginOptions)
                    .then(margin => `${data}${margin}`);
            }
            return data;
        });
}

// This function takes the raw data and breaks it into records, getting rid of anything preceding
// the first complete record if the data does not start with a complete record
function cleanData(rawData, delimiter, slice) {
    /* Since slices with a non-zero chunk offset grab the character immediately preceding the main
     * chunk, if one of those chunks has a delimiter as the first or second character, it means the
     * chunk starts with a complete record. In this case as well as when the chunk begins with a
     * partial record, splitting the chunk into an array by its delimiter will result in a single
     * garbage record at the beginning of the array. If the offset is 0, the array will never start
     * with a garbage record
     */

    if (slice.offset === 0) {
        // Return everthing
        return rawData.split(delimiter);
    }

    return rawData.split(delimiter).splice(1);
}


function schema() {
    return {
        user: {
            doc: 'User to use when reading the files. Default: "hdfs"',
            default: 'hdfs',
            format: 'optional_String'
        },
        path: {
            doc: 'HDFS location to process. Most of the time this will be a directory that contains'
                + ' multiple files',
            default: '',
            format: (val) => {
                if (typeof val !== 'string') {
                    throw new Error('path in hdfs_reader must be a string!');
                }

                if (val.length === 0) {
                    throw new Error('path in hdfs_reader must specify a valid path in hdfs!');
                }
            }
        },
        size: {
            doc: 'Determines slice size in bytes',
            default: 100000,
            format: Number
        },
        format: {
            doc: 'Format of the target file. Currently only supports "json_lines"',
            default: 'json_lines',
            format: ['json_lines']
        },
        connection: {
            doc: 'Name of the HDFS connection to use.',
            default: 'default',
            format: 'optional_String'
        }
    };
}


module.exports = {
    newReader,
    newSlicer,
    schema,
    parallelSlicers
};
