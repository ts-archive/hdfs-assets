'use strict';

/*
 * file_chunker takes an incoming stream of records and prepares them for
 * writing to a file. This is largely intended for sending data to HDFS
 * but could be used for other tasks.
 *
 * The data is collected into chunks based on 'chunk_size' and is serialized
 * to a string.
 *
 * The result of this operation is an array of objects mapping chunks to file
 * names. There can be multiple chunks for the same filename.
 * [
 *   { filename: '/path/to/file', data: 'the data'}
 * ]
 */

const _ = require('lodash');
const path = require('path');

function newProcessor(context, opConfig) {
    const config = context.sysconfig;

    return function writeData(data) {
        const buckets = {};
        let currentBucket;

        const chunks = [];

        // First we need to group the data into reasonably sized chunks as
        // specified by opConfig.chunk_size
        for (let i = 0; i < data.length; i += 1) {
            const record = data[i];
            let bucketName = '__single__';

            // If we're grouping by time we'll need buckets for each date.
            if (opConfig.timeseries) {
                bucketName = formattedDate(record, opConfig);
            }

            if (!buckets[bucketName]) {
                buckets[bucketName] = [];
            }

            currentBucket = buckets[bucketName];
            // Assumes input is a set of JSON records
            currentBucket.push(JSON.stringify(record));

            if (currentBucket.length >= opConfig.chunk_size) {
                chunks.push({
                    data: `${currentBucket.join('\n')}\n`,
                    filename: getFileName(bucketName, opConfig, config)
                });

                currentBucket = [];
                buckets[bucketName] = [];
            }
        }

        // Handle any lingering chunks.
        _.forOwn(buckets, (bucket, key) => {
            if (bucket.length > 0) {
                chunks.push({
                    data: `${bucket.join('\n')}\n`,
                    filename: getFileName(key, opConfig, config)
                });
            }
        });

        return chunks;
    };
}

function formattedDate(record, opConfig) {
    const offsets = {
        daily: 10,
        monthly: 7,
        yearly: 4
    };

    const end = offsets[opConfig.timeseries] || 10;
    const date = new Date(record[opConfig.date_field]).toISOString().slice(0, end);

    return date.replace(/-/gi, '.');
}

function getFileName(name, opConfig, config) {
    let filePath = opConfig.directory;
    if (name && (name !== '__single__')) {
        filePath = `${opConfig.directory}-${name}`;
    }

    // If filename is specified we default to this
    let filename = path.join(filePath, config._nodeName);

    if (opConfig.filename) {
        filename = path.join(filePath, opConfig.filename);
    }

    return filename;
}

function schema() {
    return {
        timeseries: {
            doc: 'Set to an interval to have directories named using a date field from the data '
            + 'records.',
            default: null,
            format: ['daily', 'monthly', 'yearly', null]
        },
        date_field: {
            doc: 'Which field in each data record contains the date to use for timeseries. Only '
            + 'useful if "timeseries" is also specified.',
            default: 'date',
            format: String
        },
        directory: {
            doc: 'Path to use when generating the file name. Default: /',
            default: '/',
            format: String
        },
        filename: {
            doc: 'Filename to use. This is optional and is not recommended if the target is HDFS. '
            + 'If not specified a filename will be automatically chosen to reduce the occurence of '
            + 'concurrency issues when writing to HDFS.',
            default: '',
            format: 'optional_String'
        },
        chunk_size: {
            doc: 'Size of the data chunks. Specifies the number of records to include in a chunk. '
            + 'A new chunk will be created when this size is surpassed. Default: 50000',
            default: 50000,
            format: (val) => {
                if (isNaN(val)) {
                    throw new Error('size parameter for chunk_size must be a number!');
                } else if (val <= 0) {
                    throw new Error('size parameter for chunk_size must be greater than zero!');
                }
            }
        }
    };
}

module.exports = {
    newProcessor,
    schema
};
