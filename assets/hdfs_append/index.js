'use strict';

const _ = require('lodash');
const Promise = require('bluebird');
const path = require('path');


function newProcessor(context, opConfig, jobConfig) {
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
                    return Promise.reject(
                        `Error while attempting to create the file: ${filename} on hdfs, error: `
                        + `${errMsg}`
                    );
                }))
            .return(chunks)
            // We need to serialize the storage of chunks so we run with concurrency 1
            .map(chunks, chunk => hdfsClient.appendAsync(filename, chunk), { concurrency: 1 })
            .catch((err) => {
                const errMsg = err.stack ? err.stack : err;
                let sliceError = '';
                // Detecting the hdfs append error and updating the filename
                if (errMsg.indexOf('AlreadyBeingCreatedException') > -1) {
                    let newFilename = '';
                    if (!appendErrors[filename]) {
                        newFilename = `${filename}.0`;
                        appendErrors[filename] = newFilename;
                    } else {
                        // Get the last attempted file and increment the number
                        const incNum = appendErrors[filename].split('.').reverse()[0] * 1 + 1;
                        newFilename = `${filename}.${incNum}`;
                        // Set the new target for the next slice attempt
                        appendErrors[filename] = newFilename;
                    }
                    sliceError = `Error sending data to file '${filename}' due to HDFS append `
                        + `error. Changing destination to '${newFilename}'. Error: ${errMsg}`;
                } else {
                    sliceError = `Error sending data to file: ${filename}, error: ${errMsg}`;
                }
                if (opConfig.log_data_on_error === true) {
                    sliceError = `${sliceError} Data: ${JSON.stringify(chunks)}`;
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
                let file = record.filename;
                // If the file has already had an error, update the filename for the next write
                // attempt
                if (appendErrors[file]) {
                    const attemptNum = appendErrors[file].split('.').reverse()[0] * 1;
                    // This stops the worker from creating too many new files
                    if (attemptNum > opConfig.max_writes) {
                        throw new Error(
                            `${file} has exceeded the maximum number of write attempts!`
                        );
                    }
                    file = appendErrors[file];
                }
                if (!map[file]) map[file] = [];
                map[file].push(record.data);
            }
        });

        function sendFiles() {
            const stores = [];
            _.forOwn(map, (chunks, key) => {
                stores.push(prepareFile(key, chunks, jobConfig.logger));
            });

            // We can process all individual files in parallel.
            return Promise.all(stores)
                .catch((err) => {
                    const errMsg = err.stack ? err.stack : err;
                    jobConfig.logger.error(`Error while sending to hdfs, error: ${errMsg}`);
                    return Promise.reject(err);
                });
        }

        return sendFiles();
    };
}

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
        max_writes: {
            doc: 'Determines how many times a worker can create a new file after append errors '
                + 'before throwing an error on every slice attempt. Defaults to 100',
            default: 100,
            format: (val) => {
                if (isNaN(val)) {
                    throw new Error('size parameter for max_writes must be a number!');
                } else if (val <= 0) {
                    throw new Error('size parameter for max_writes must be greater than zero!');
                }
            }
        }
    };
}

module.exports = {
    newProcessor,
    schema
};
