'use strict';

const reader = require('../asset/hdfs_reader');

describe('hdfs_reader', () => {
    it('should export the required files', () => {
        expect(reader.newReader).toBeFunction();
        expect(reader.newSlicer).toBeFunction();
        expect(reader.schema).toBeFunction();
    });
});
