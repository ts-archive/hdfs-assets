'use strict';

const Promise = require('bluebird');
const Queue = require('@terascope/queue');
const chunkSettings = require('../lib/chunk_settings');
const chunkFormater = require('../lib/chunk_formatter');


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
                // not the chunk starts with a complete record. If this is done, the length also
                // needs to increase by 1 to ensure no data is lost
                let startSpot = offset;
                let readLength = length;
                if (offset > 0) {
                    startSpot = offset - 1;
                    readLength = length + 1;
                }
                queue.enqueue({
                    path: `${filePath}/${file.pathSuffix}`,
                    offset: startSpot,
                    length: readLength,
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
    // Set up a logger
    const logger = context.apis.foundation.makeLogger({ module: 'hdfs_reader' });
    const clientService = getClient(context, opConfig, 'hdfs_ha');
    const hdfsClient = clientService.client;

    return function processSlice(slice) {
        return new Promise(
            resolve => resolve(chunkSettings.getReadOptions(slice, opConfig))
        )
            .then(readOptions => readFile(hdfsClient, readOptions, false))
            .then(readOptions => chunkSettings.checkMargin(readOptions))
            .then((readOptions) => {
                if (readOptions.needMargin) {
                    return readFile(hdfsClient, readOptions, readOptions.needMargin);
                }
                return readOptions;
            })
            .then(readOptions => chunkSettings.cleanData(readOptions))
            .then(data => chunkFormater[opConfig.format](data, logger))
            .catch(err => Promise.reject(parseError(err)));
    };
}

// This function will just read a chunk from the file and add it to `readOptions.data`. `isMargin`
// just tells the function if data needs to be cut before being added to the payload
function readFile(hdfsClient, readOptions, isMargin) {
    if (isMargin) {
        return hdfsClient.openAsync(readOptions.path, readOptions.marginOptions)
            .then((data) => {
                readOptions.data = `${readOptions.data}${data.split(readOptions.delimiter)[0]}`;
                return readOptions;
            });
    }
    return hdfsClient.openAsync(readOptions.path, readOptions.options)
        .then((data) => {
            readOptions.data = data;
            return readOptions;
        });
}

function parseError(err) {
    if (err.message && err.exception) {
        return `Error while reading from HDFS, error: ${err.exception}, ${err.message}`;
    }
    return `Error while reading from HDFS, error: ${err}`;
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
            default: 'jsonLines',
            format: ['jsonLines']
        },
        connection: {
            doc: 'Name of the HDFS connection to use.',
            default: 'default',
            format: 'optional_String'
        },
        line_delimiter: {
            doc: 'Specifies the line delimiter for the file(s) being read. Currently `\n` is the '
            + 'only supported delimiter.',
            default: '\n',
            format: ['\n']
        }
    };
}


module.exports = {
    newReader,
    newSlicer,
    schema,
    parallelSlicers
};
