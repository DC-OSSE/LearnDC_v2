/*jslint node: true*/
/*jslint sloppy: true*/
/*jslint nomen: true*/
/*global describe, before, it*/

var _ = require('lodash'),
    assert = require('assert'),
    fs = require('fs'),
    glob = require('glob');

var EXPORT = 'Export/JSON/';

var EXHIBITS = [
    'attendance.json',
    'dccas.json',
    'enrollment_equity.json',
    'enrollment.json',
    'expulsions.json',
    'graduation.json',
    'mgp_scores.json',
    'mid_year_entry_and_withdrawal.json',
    'suspensions.json',
    'unexcused_absences.json'
];

function removeHiddenFiles(list) {
    return _.filter(list, function (item) {
        return item[0] !== '.';
    });
}

describe('LearnDC data files', function () {
    var jsonFiles;

    before(function () {
        jsonFiles = glob.sync(EXPORT + '/**/*.+(json|JSON)');
    });

    describe('Directory structure', function () {

        it('all JSON files should be in a subdirectory', function () {
            _.each(jsonFiles, function (file) {
                assert.equal(file.split('/').length, 5, file + ' is not at the correct depth.');
            });
        });

        it('all JSON files should match a known exhibit type', function () {
            _.each(jsonFiles, function (file) {
                assert(_.difference(_.last(file.split('/')), EXHIBITS).length, 0, file + 'is not a known exhibit type.');
            });
        });

        it('should have a school, lea and state directory', function (done) {
            fs.readdir(EXPORT, function (err, list) {
                if (err) { throw err; }
                list = removeHiddenFiles(list);
                if (list.length === 3 || _.difference(list, ['school', 'lea', 'state']).length === 3) {
                    done();
                }
            });
        });

        it('school directories should be four-digit codes', function () {
            fs.readdir(EXPORT + 'school', function (err, list) {
                if (err) { throw err; }
                list = removeHiddenFiles(list);
                _.each(list, function (file) {
                    if (file.length !== 4 || isNaN(parseInt(file, 10))) {
                        throw new Error('School code ' + file + ' is not a four digit number.');
                    }
                });
            });
        });

        it('lea directories should be four-digit codes', function () {
            fs.readdir(EXPORT + 'lea', function (err, list) {
                if (err) { throw err; }
                list = removeHiddenFiles(list);
                _.each(list, function (file) {
                    if (file.length !== 4 || isNaN(parseInt(file, 10))) {
                        throw new Error('LEA code ' + file + ' is not a four digit number.');
                    }
                });
            });
        });

        it('state data should be in a folder called "DC"', function (done) {
            fs.readdir(EXPORT + 'state', function (err, list) {
                if (err) { throw err; }
                list = removeHiddenFiles(list);
                if (list.length !== 1) { throw new Error('There are multiple directories in "state".'); }
                if (list[0] === 'DC') { done(); }
            });
        });
    });

    describe('JSON', function () {

        it('all JSON files should be valid JSON', function () {
            _.each(jsonFiles, function (file) {
                fs.readFile(file, function (err, json) {
                    if (err) { throw err; }
                    try {
                        JSON.parse(json);
                    } catch (e) {
                        throw new Error('File ' + file + ' is not valid JSON. ' + e);
                    }
                });
            });
        });
    });
});