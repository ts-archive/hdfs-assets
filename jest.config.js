'use strict';

module.exports = {
    rootDir: '.',
    testMatch: [
        '<rootDir>/spec/*-spec.js'
    ],
    collectCoverage: true,
    collectCoverageFrom: [
        '<rootDir>/asset/**/*.js',
        '<rootDir>/asset/*/*.js',
        '!<rootDir>/asset/node_modules',
    ],
    coverageReporters: ['lcov', 'text-summary', 'html'],
    coverageDirectory: '<rootDir>/coverage'
};
