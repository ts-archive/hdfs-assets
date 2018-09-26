'use strict';

const hdfsAppend = require('../assets/hdfs_append/index.js');


// Start out with no file errors
const appendErrors = {};

describe('The hdfs_append processor', () => {
    it('properly passes a clean filename through.', () => {
        const filename = '/incoming-data/2037-2-27/ts-worker.lan.42';
        const newName = hdfsAppend._checkFileHistory(filename, appendErrors, 100);
        expect(newName).toEqual(filename);
        expect(Object.keys(appendErrors).length).toEqual(0);
    });
    it('properly changes the filename after an HDFS append error.', () => {
        const filename = '/incoming-data/2037-2-27/ts-worker.lan.42';
        const newName = hdfsAppend._recordFileError(filename, appendErrors);
        expect(newName).toEqual('/incoming-data/2037-2-27/ts-worker.lan.42.0');
        expect(Object.keys(appendErrors).length).toEqual(2);
        expect(appendErrors.retry).toEqual(true);
        expect(appendErrors['/incoming-data/2037-2-27/ts-worker.lan.42'])
            .toEqual('/incoming-data/2037-2-27/ts-worker.lan.42.0');
    });
    it('properly changes the filename after a second HDFS append error.', () => {
        const filename = '/incoming-data/2037-2-27/ts-worker.lan.42.0';
        const newName = hdfsAppend._recordFileError(filename, appendErrors);
        expect(newName).toEqual('/incoming-data/2037-2-27/ts-worker.lan.42.1');
        expect(Object.keys(appendErrors).length).toEqual(2);
        expect(appendErrors.retry).toEqual(true);
        expect(appendErrors['/incoming-data/2037-2-27/ts-worker.lan.42'])
            .toEqual('/incoming-data/2037-2-27/ts-worker.lan.42.1');
    });
    it('properly passes a clean filename for the next day.', () => {
        const filename = '/incoming-data/2037-2-28/ts-worker.lan.42';
        const newName = hdfsAppend._checkFileHistory(filename, appendErrors, 100);
        expect(newName).toEqual(filename);
        expect(appendErrors.retry).toEqual(true);
        expect(appendErrors['/incoming-data/2037-2-27/ts-worker.lan.42'])
            .toEqual('/incoming-data/2037-2-27/ts-worker.lan.42.1');
    });
    it('properly changes the filename after an HDFS append error for the new file.', () => {
        const filename = '/incoming-data/2037-2-28/ts-worker.lan.42';
        const newName = hdfsAppend._recordFileError(filename, appendErrors);
        expect(newName).toEqual('/incoming-data/2037-2-28/ts-worker.lan.42.0');
        expect(appendErrors.retry).toEqual(true);
        expect(appendErrors['/incoming-data/2037-2-28/ts-worker.lan.42'])
            .toEqual('/incoming-data/2037-2-28/ts-worker.lan.42.0');
    });
});
