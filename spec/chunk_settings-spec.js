'use strict';

const chunkSettings = require('../assets/lib/chunk_settings');


const marginSlice = {
    path: '/some/test/file',
    offset: 0,
    length: 10,
    total: 20
};
const endSlice = {
    path: '/some/test/file',
    offset: 10,
    length: 10,
    total: 20
};

// Only thing needed from the opConfig is the line delimiter
const opConfig = {
    line_delimiter: '\n'
};

const chunkOptionsComplete = {
    data: 'some test data\n',
    needMargin: false
};

const chunkOptionsMargin = {
    data: 'some\nmore\ntest\ndata',
    delimiter: '\n',
    needMargin: true,
    options: {
        offset: 10,
        length: 10
    }
};

const chunkOptionsStart = {
    data: 'some\nmore\ntest\ndata\n',
    delimiter: '\n',
    needMargin: true,
    options: {
        offset: 0,
        length: 10
    }
};

describe('The chunk_settings module', () => {
    it('generates an accurate chunk configuration object.', () => {
        const marginChunk = chunkSettings.getReadOptions(marginSlice, opConfig);
        const endChunk = chunkSettings.getReadOptions(endSlice, opConfig);
        expect(marginChunk.needMargin).toEqual(true);
        expect(endChunk.needMargin).toEqual(false);
        expect(marginChunk.delimiter).toEqual('\n');
        expect(marginChunk.path).toEqual('/some/test/file');
        expect(marginChunk.data).toEqual({});
        expect(marginChunk.options.offset).toEqual(0);
        expect(marginChunk.options.length).toEqual(10);
    });
    it('detects data ending in a complete record.', () => {
        const completeChunk = chunkSettings.checkMargin(chunkOptionsComplete);
        expect(completeChunk.marginOptions).toEqual(undefined);
    });
    it('properly configures margin settings.', () => {
        const marginChunk = chunkSettings.checkMargin(chunkOptionsMargin);
        expect(marginChunk.marginOptions.offset).toEqual(20);
        expect(marginChunk.marginOptions.length).toEqual(8);
    });
    it('properly breaks down data into records', () => {
        const startChunk = chunkSettings.cleanData(chunkOptionsStart);
        const middleChunk = chunkSettings.cleanData(chunkOptionsMargin);
        expect(startChunk).toEqual(['some', 'more', 'test', 'data']);
        expect(middleChunk).toEqual(['more', 'test', 'data']);
    });
});
