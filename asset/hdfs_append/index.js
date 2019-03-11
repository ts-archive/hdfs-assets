'use strict';

const _ = require('lodash');
const Promise = require('bluebird');
const path = require('path');
const { TSError } = require('@terascope/utils');


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

// Records the name of the offending file with a detected corrupt block in `appendErrors`
function _recordFileError(name, errorLog) {
    let newFilename = '';
    // Get the original file name from the error message. If there have been no previous errors
    // for the given file, this will result in a bogus filename that will trigger the second
    // block of the following checks. Otherwise, the third block will be used to get a new name
    const fileBase = name.substring(0, name.lastIndexOf('.'));
    if (!errorLog.retry) {
        // In this case, there has not been any hdfs append errors for a worker

        newFilename = `${name}.0`;
        errorLog[name] = newFilename;
        // Ensures this block will not be executed again after the first error for a file
        errorLog.retry = true;
    } else if (!errorLog[fileBase]) {
        // In this case, there has been at least one hdfs append error for a worker, but the
        // worker is now writing to a new file

        // Clean out the appendErrors object to avoid an accumulation of filenames
        Object.keys(errorLog).forEach((key) => {
            delete errorLog[key];
        });

        newFilename = `${name}.0`;
        errorLog[name] = newFilename;
        // Add the `retry` key back in
        errorLog.retry = true;
    } else {
        // In this case, there has been at least one hdfs append error for the given file

        // Get the last attempted file and increment the number
        const incNum = +errorLog[fileBase].split('.').slice(-1) + 1;
        newFilename = `${fileBase}.${incNum}`;
        // Set the new target for the next slice attempt
        errorLog[fileBase] = newFilename;
    }
    return newFilename;
}

// This just checks `appendErrors` for the file to determine if data needs to be redirected to
// the new file
function _checkFileHistory(name, errorLog, maxWrites) {
    // If the file has already had an error, update the filename for the next write
    // attempt
    if (errorLog[name]) {
        const attemptNum = +errorLog[name].split('.').slice(-1);
        // This stops the worker from creating too many new files
        if (attemptNum > maxWrites) {
            throw new TSError(`${name} has exceeded the maximum number of write attempts!`);
        }
        return errorLog[name];
    }
    return name;
}

function newProcessor(context, opConfig) {
    const logger = context.apis.foundation.makeLogger({ module: 'hdfs_append' });
    // Client connection cannot be cached, an endpoint needs to be re-instantiated for a different
    // namenode_host
    opConfig.connection_cache = false;

    /* This is used to keep track of HDFS append errors caused by corrupted replicas. If an error
    /* occurs, a suffix will be added to the filename and incremented for each subsequent error. The
    /* slice retry in a normal production job should take long enough to prevent the TS worker from
    /* smashing the Namenode, but a timer mechanism will still need to be implemented in order to
    /* prevent this from happening with faster-moving jobs. During the slice retry, this processor
    /* will end up seeing the incremented filename in the `appendErrors` object and use the updated
    /* name instead
     */
    // TODO: implement the timing mechanism. Current assumption is that retry and new slice
    //       processing duration will keep the worker from toppling the Namenode
    const appendErrors = {};


    const clientService = getClient(context, opConfig, 'hdfs_ha');
    const hdfsClient = clientService.client;

    function prepareFile(filename, chunks) {
        // We need to make sure the file exists before we try to append to it.
        return hdfsClient.getFileStatusAsync(filename)
            .catch(() => hdfsClient.mkdirsAsync(path.dirname(filename))
                .then(() => hdfsClient.createAsync(filename, ''))
                .catch((err) => {
                    const errMsg = err.stack;
                    return Promise.reject(new TSError(errMsg, {
                        reason: 'Error while attempting to create a file',
                        context: {
                            filename
                        }
                    }));
                }))
            .return(chunks)
            // We need to serialize the storage of chunks so we run with concurrency 1
            .map(chunk => hdfsClient.appendAsync(filename, chunk), { concurrency: 1 })
            .catch((err) => {
                const errMsg = err.stack ? err.stack : err;
                let sliceError = '';
                /* Detecting the hdfs append error caused by block relocation and updating the
                /* filename. The `AlreadyBeingCreatedException` error is caused by something else
                /* and needs to be investigated further before implementing a fix. The error caused
                /* by the block relocation manifests itself as a stacktrace pointing at the file in
                /* this check.
                 */
                if (errMsg.indexOf('remoteexception.js') > -1) {
                    const newFilename = _recordFileError(filename, appendErrors);
                    sliceError = new TSError(errMsg, {
                        reason: 'Changing file name due to HDFS append error',
                        context: {
                            file: filename,
                            new_file: newFilename
                        }
                    });
                } else {
                    sliceError = new TSError(errMsg, {
                        reason: 'Error sending data to file',
                        context: {
                            file: filename
                        }
                    });
                }
                if (opConfig.log_data_on_error === true) {
                    sliceError.context.data = JSON.stringify(chunks);
                }
                return Promise.reject(sliceError);
            });
    }

    return (data) => {
        // Start by mapping data chunks to their respective files
        const map = {};
        data.forEach((record) => {
            // This skips any records that have non-existant data payloads to avoid empty appends
            if (record.data.length > 0) {
                const file = _checkFileHistory(
                    record.filename,
                    appendErrors,
                    opConfig.max_write_errors
                );
                if (!map[file]) map[file] = [];
                map[file].push(record.data);
            }
        });

        function sendFiles() {
            const stores = [];
            _.forOwn(map, (chunks, key) => {
                stores.push(prepareFile(key, chunks, logger));
            });

            // We can process all individual files in parallel.
            return Promise.all(stores)
                .catch((err) => {
                    const errMsg = err.stack ? err.stack : err;
                    const error = new TSError(errMsg, {
                        reason: 'Error sending data to file'
                    });
                    return Promise.reject(error);
                });
        }

        return sendFiles();
    };
}

function schema() {
    // Most important schema configs are in the connection configuration
    return {
        connection: {
            doc: 'Name of the HDFS connection to use.',
            default: 'default',
            format: 'optional_String'
        },
        log_data_on_error: {
            doc: 'Determines whether or not to include the data for the slice in error messages',
            default: false,
            format: Boolean
        },
        max_write_errors: {
            doc: 'Determines how many times a worker can create a new file after append errors '
                + 'before throwing an error on every slice attempt. Defaults to 100',
            default: 100,
            format: (val) => {
                if (isNaN(val)) {
                    throw new Error('size parameter for max_write_errors must be a number!');
                } else if (val <= 0) {
                    throw new Error('size parameter for max_write_errors must be greater than zero!');
                }
            }
        }
    };
}

module.exports = {
    newProcessor,
    _recordFileError,
    _checkFileHistory,
    schema
};
