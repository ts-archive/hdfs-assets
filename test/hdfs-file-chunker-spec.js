'use strict';

const processor = require('../asset/hdfs_file_chunker');

describe('hdfs_file_chunker', () => {
    it('should export the required files', () => {
        expect(processor.newProcessor).toBeFunction();
        expect(processor.schema).toBeFunction();
    });
});
